import streamlit as st
from neo4j import GraphDatabase
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import altair as alt
import plotly.graph_objects as go


URI = "neo4j+ssc://3dddc413.databases.neo4j.io" 
AUTH = ("neo4j", "uT5Hwz7OKdMBuE4mb8WPSz0AmKu0yY57zacumyaifZc")

st.set_page_config(page_title="Bitcoin Forensics", layout="wide", initial_sidebar_state="collapsed")

# --- 2. GESTIÓN DE DATOS ---
def run_query(query, params=None):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(query, params)
            return [r.data() for r in result]

@st.cache_data
def get_final_data():
    # A. KPIs Básicos
    q_kpis = """
    MATCH (t:Transaction)
    RETURN 
        count(t) as Total,
        sum(CASE WHEN t.class = '1' THEN 1 ELSE 0 END) as Ilicitas,
        sum(CASE WHEN t.class = 'unknown' THEN 1 ELSE 0 END) as Desconocidas
    """
    
    # B. Promedio de Vecinos (Ilícitos) -> SUMA Entradas y Salidas (Undirected)
    # El patrón -[r]- sin flecha indica dirección indistinta (Entrada + Salida)
    q_avg_degree = """
    MATCH (t:Transaction {class: '1'})-[r]-()
    RETURN count(r) * 1.0 / count(DISTINCT t) as Promedio
    """

    # C. Time Step con MAYOR grado de ilícito
    q_max_step = """
    MATCH (t:Transaction) 
    WHERE t.class = '1'
    RETURN t.time_step as Step, count(t) as Cantidad
    ORDER BY Cantidad DESC
    LIMIT 1
    """

    # D. NUEVO: Mayor Centralidad REAL (Suma Entradas + Salidas)
    q_total_degree_max = """
    MATCH (t:Transaction)-[r]-()
    RETURN t.tx_id as ID, count(r) as TotalDegree
    ORDER BY TotalDegree DESC
    LIMIT 1
    """

    # E. Evolución Comparativa
    q_time_compare = """
    MATCH (t:Transaction) 
    WHERE t.class IN ['1', '2']
    RETURN toInteger(t.time_step) as Step, t.class as Clase, count(t) as Cantidad
    ORDER BY Step ASC
    """

    # F. Afinidad / Destino
    q_affinity = """
    MATCH (source:Transaction {class: '1'})-[r]->(target:Transaction)
    RETURN target.class as Destino, count(r) as Pagos
    ORDER BY Pagos DESC
    """
    
    # G. Grafo
    q_graph = """
    MATCH (a:Transaction)-[r]->(b:Transaction)
    WHERE a.class = '1' OR b.class = '1'
    RETURN a.tx_id as source, b.tx_id as target, a.class as class_source, b.class as class_target
    LIMIT 40
    """

    # H. Tablas Top (Se mantienen separadas para el detalle final)
    q_out = "MATCH (t:Transaction)-[r]->() RETURN t.tx_id as ID, t.class as Clase, count(r) as Envios ORDER BY Envios DESC LIMIT 5"
    q_in = "MATCH ()-[r]->(t:Transaction) RETURN t.tx_id as ID, t.class as Clase, count(r) as Recepciones ORDER BY Recepciones DESC LIMIT 5"

    # --- EJECUCIÓN ---
    kpis = run_query(q_kpis)[0]
    avg_degree = run_query(q_avg_degree)[0]['Promedio']
    max_step_data = run_query(q_max_step)
    max_cent_data = run_query(q_total_degree_max) # Nueva query
    
    df_compare = pd.DataFrame(run_query(q_time_compare))
    df_affinity = pd.DataFrame(run_query(q_affinity))
    df_graph = pd.DataFrame(run_query(q_graph))
    df_out = pd.DataFrame(run_query(q_out))
    df_in = pd.DataFrame(run_query(q_in))
    
    # Cálculos
    total_identificado = kpis['Total'] - kpis['Desconocidas']
    tasa_ilicita = (kpis['Ilicitas'] / total_identificado * 100) if total_identificado > 0 else 0
    
    peak_step = max_step_data[0]['Step'] if max_step_data else "N/A"
    peak_qty = max_step_data[0]['Cantidad'] if max_step_data else 0
    
    # El nodo con mayor grado TOTAL
    max_centralidad = max_cent_data[0]['TotalDegree'] if max_cent_data else 0

    return {
        "kpis": kpis,
        "tasa": tasa_ilicita,
        "avg_degree": avg_degree,
        "peak_step": peak_step,
        "peak_qty": peak_qty,
        "max_cent": max_centralidad,
        "df_compare": df_compare,
        "df_affinity": df_affinity,
        "df_graph": df_graph,
        "df_out": df_out,
        "df_in": df_in
    }

# --- 3. INTERFAZ GRÁFICA ---
st.title("🕵️ Dashboard Forense Bitcoin (Elliptic Data)")
st.markdown("---")

# ------- CRUD Interface -------
st.subheader("⚙️ Operaciones CRUD")

with st.expander("⚡ Administrar Transacciones"):
    action = st.selectbox(
        "Selecciona la operación",
        ["Crear", "Leer", "Actualizar", "Eliminar"]
    )

    # --- CREATE ---
    if action == "Crear":
        st.write("Crear nueva transacción (features opcional)")
        tx_id = st.text_input("tx_id", key="create_txid")
        time_step = st.number_input("time_step", step=1, key="create_step")
        clase = st.text_input("class", key="create_class")
        features_input = st.text_area(
            "features (opcional) — lista separada por comas, ej: 0.1, -0.23, 1.0",
            key="create_features"
        )

        if st.button("Crear"):
            # Parsear features si hay
            params = {"tx_id": str(tx_id), "time_step": int(time_step), "clase": str(clase)}
            if features_input and features_input.strip():
                try:
                    feats = [float(x.strip()) for x in features_input.split(",") if x.strip() != ""]
                    params["features"] = feats
                    # Query con features
                    q = """
                    CREATE (t:Transaction {
                        tx_id:$tx_id, time_step:$time_step, class:$clase, features:$features
                    })
                    """
                except Exception as e:
                    st.error(f"Error parseando features: {e}")
                    st.stop()
            else:
                # Query sin features
                q = """
                CREATE (t:Transaction {
                    tx_id:$tx_id, time_step:$time_step, class:$clase
                })
                """
            run_query(q, params)
            # Limpiar cache para que lo que muestra el dashboard se actualice
            try:
                get_final_data.clear()
            except:
                pass
            st.success("Transacción creada ✔")

    # --- READ ---
    elif action == "Leer":
        st.write("Consultar transacciones")

        # Link RAW
        st.markdown("[📦 Ver dataset completo (RAW)](https://github.com/apparent-batman/to_neo4j)")

        # Obtener lista de tx_id
        ids = run_query("MATCH (t:Transaction) RETURN t.tx_id AS id LIMIT 500")
        id_list = [row["id"] for row in ids if row.get("id")]

        # Agregar opción para ingresar manual
        choice = st.selectbox("Seleccionar tx_id o ingresar manualmente", ["-- Ingresar manual --"] + id_list)
        if choice == "-- Ingresar manual --":
            selected_id = st.text_input("Escribe tx_id aquí", key="manual_id")
        else:
            selected_id = choice

        if st.button("Buscar"):
            if not selected_id:
                st.warning("Ingresa o selecciona un tx_id.")
            else:
                res = run_query("MATCH (t:Transaction {tx_id:$id}) RETURN t", {"id": selected_id})
                if res:
                    st.json(res[0]["t"])
                else:
                    st.info("No encontrado")

    # --- UPDATE ---
    elif action == "Actualizar":
        st.write("Actualizar transacción existente")
        tx_id_up = st.text_input("tx_id a actualizar", key="up_id")
        nueva_clase = st.text_input("Nuevo valor de class", key="up_class")
        if st.button("Actualizar"):
            if not tx_id_up:
                st.warning("Ingresa tx_id a actualizar.")
            else:
                run_query("""
                MATCH (t:Transaction {tx_id:$id})
                SET t.class = $nueva_clase
                """, {"id": tx_id_up, "nueva_clase": nueva_clase})
                try:
                    get_final_data.clear()
                except:
                    pass
                st.success("Transacción actualizada ✔")

    # --- DELETE ---
    elif action == "Eliminar":
        st.write("Eliminar transacción")
        tx_id_del = st.text_input("tx_id a eliminar", key="del_id")
        if st.button("Eliminar"):
            if not tx_id_del:
                st.warning("Ingresa tx_id a eliminar.")
            else:
                run_query("MATCH (t:Transaction {tx_id:$id}) DETACH DELETE t", {"id": tx_id_del})
                try:
                    get_final_data.clear()
                except:
                    pass
                st.error("Transacción eliminada ❌")


# ---------------------------------------------------

try:
    data = get_final_data()
    kpis = data['kpis']

    # === NIVEL 1: KPIs ===
    c1, c2, c3, c4, c5 = st.columns(5)
    
    c1.metric("📦 Total Transacciones", f"{kpis['Total']:,}")
    
    c2.metric("⚠️ Tasa Ilícita", f"{data['tasa']:.2f}%", 
              help="Sobre el total clasificado.")
    
    c3.metric("🕸️ Mayor Centralidad", f"{data['max_cent']}", 
              help="Suma total de conexiones (Entradas + Salidas) del nodo más activo.")
    
    c4.metric("📅 Pico de Fraude", f"Semana {data['peak_step']}", 
              help=f"Time step con mayor volumen ({data['peak_qty']} txs).")
    
    c5.metric("🔗 Promedio Vecinos", f"{data['avg_degree']:.2f}", 
              help="Promedio de conexiones totales (In+Out) de los nodos ilícitos.")

    st.markdown("---")

    # === NIVEL 2: GRÁFICOS ===
    col_evo, col_pie, col_aff = st.columns([1.5, 0.8, 1.2])

    # 1. Evolución
    with col_evo:
        st.subheader("📈 Dinámica Transaccional")
        if not data['df_compare'].empty:
            label_map = {'1': 'Ilícita', '2': 'Lícita'}
            data['df_compare']['Tipo'] = data['df_compare']['Clase'].map(label_map)
            
            chart = alt.Chart(data['df_compare']).mark_line().encode(
                x=alt.X('Step', title='Time Step'),
                y=alt.Y('Cantidad', title='Volumen'),
                color=alt.Color('Tipo', scale=alt.Scale(domain=['Ilícita', 'Lícita'], range=['#FF4B4B', '#2ECC71'])),
                tooltip=['Step', 'Tipo', 'Cantidad']
            ).interactive()
            st.altair_chart(chart, use_container_width=True)

    # 2. Distribución
    with col_pie:
        st.subheader("🍰 Distribución")
        vals = [kpis['Ilicitas'], kpis['Total'] - kpis['Ilicitas'] - kpis['Desconocidas'], kpis['Desconocidas']]
        labels = ['Ilícitas', 'Lícitas', 'Unknown']
        cols = ['#FF4B4B', '#2ECC71', '#BDC3C7']
        
        def make_autopct(values):
            def my_autopct(pct):
                total = sum(values)
                val = int(round(pct*total/100.0))
                return '{p:.1f}%\n({v:d})'.format(p=pct, v=val) if pct > 2 else ''
            return my_autopct

        fig1, ax1 = plt.subplots(figsize=(4, 4))
        ax1.pie(vals, labels=labels, colors=cols, autopct=make_autopct(vals), startangle=140, textprops={'fontsize': 8})
        st.pyplot(fig1, use_container_width=True)

    # 3. Afinidad
    with col_aff:
        st.subheader("💸 Flujo de Fondos")
        if not data['df_affinity'].empty:
            mapa = {'1': 'A Ilícito', '2': 'A Lícito', 'unknown': 'A Unknown'}
            data['df_affinity']['Destino_Nombre'] = data['df_affinity']['Destino'].map(mapa)
            
            chart_bar = alt.Chart(data['df_affinity']).mark_bar().encode(
                x=alt.X('Destino_Nombre', sort='-y', title='Destinatario'),
                y=alt.Y('Pagos', title='Cant. TXs'),
                color=alt.Color('Destino', scale=alt.Scale(domain=['1', '2', 'unknown'], range=['#FF4B4B', '#2ECC71', '#95A5A6'])),
                tooltip=['Destino_Nombre', 'Pagos']
            )
            st.altair_chart(chart_bar, use_container_width=True)

    st.markdown("---")

    # === NIVEL 3: TABLAS Y RED ===
    c_tab1, c_tab2, c_graph = st.columns([1, 1, 1.5])

    with c_tab1:
        st.markdown("#### 📤 Top 5 Emisores")
        st.caption("Mayor Grado de Salida (Out-Degree)")
        st.dataframe(data['df_out'][['ID', 'Envios', 'Clase']], use_container_width=True, hide_index=True)

    with c_tab2:
        st.markdown("#### 📥 Top 5 Receptores")
        st.caption("Mayor Grado de Entrada (In-Degree)")
        st.dataframe(data['df_in'][['ID', 'Recepciones', 'Clase']], use_container_width=True, hide_index=True)

    with c_graph:
        st.markdown("#### 🕸️ Red Forense (Interactivo)")
        df_g = data['df_graph']
        if not df_g.empty:
            G = nx.from_pandas_edgelist(df_g, source='source', target='target')
            pos = nx.spring_layout(G, seed=42, k=0.3)

            edge_x = []
            edge_y = []
            for edge in G.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])

            edge_trace = go.Scatter(
                x=edge_x, y=edge_y,
                line=dict(width=0.5, color='#888'),
                hoverinfo='none',
                mode='lines')

            node_x = []
            node_y = []
            node_text = []
            node_color = []
            
            # Mapeo de clases
            all_nodes = pd.concat([
                df_g[['source', 'class_source']].rename(columns={'source':'id', 'class_source':'cls'}),
                df_g[['target', 'class_target']].rename(columns={'target':'id', 'class_target':'cls'})
            ]).drop_duplicates(subset='id').set_index('id')

            for node in G.nodes():
                x, y = pos[node]
                node_x.append(x)
                node_y.append(y)
                try:
                    c = all_nodes.loc[node]['cls']
                    col ='#FF4B4B' if c == '1' else ('#2ECC71' if c == '2' else '#BDC3C7')
                    txt = "Ilícito" if c == '1' else ("Lícito" if c == '2' else "Unknown")
                except:
                    col = '#BDC3C7'; txt = "?"
                
                node_text.append(f"ID: {node}<br>Tipo: {txt}")
                node_color.append(col)

            node_trace = go.Scatter(
                x=node_x, y=node_y,
                mode='markers',
                hoverinfo='text',
                text=node_text,
                marker=dict(showscale=False, color=node_color, size=10, line_width=1))

            fig = go.Figure(data=[edge_trace, node_trace],
                            layout=go.Layout(
                                showlegend=False,
                                hovermode='closest',
                                margin=dict(b=0,l=0,r=0,t=0),
                                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                            )
            st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error: {e}")