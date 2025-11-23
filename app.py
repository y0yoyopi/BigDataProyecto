import streamlit as st
from neo4j import GraphDatabase
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


# Configuracion de la pagina
st.set_page_config(
    page_title="Bitcoin Forensics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuracion de Neo4j
URI = NEO4J_URI
AUTH = ("neo4j", NEO4J_PASSWORD)


# Funciones de conexion
def run_query(query, params=None):
    """Ejecuta una consulta en Neo4j y retorna los resultados"""
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(query, params or {})
            return [r.data() for r in result]


def query_to_df(query, params=None):
    """Ejecuta una consulta y retorna un DataFrame"""
    data = run_query(query, params)
    return pd.DataFrame(data)


# Cache de datos
@st.cache_data(ttl=300)
def get_basic_stats():
    """Obtiene estadisticas basicas del dataset"""
    query = """
    MATCH (t:Transaction)
    RETURN 
        count(t) as total_transactions,
        count(DISTINCT t.time_step) as unique_timesteps,
        min(toInteger(t.time_step)) as min_timestep,
        max(toInteger(t.time_step)) as max_timestep,
        sum(CASE WHEN t.class = '1' THEN 1 ELSE 0 END) as illicit_count,
        sum(CASE WHEN t.class = '2' THEN 1 ELSE 0 END) as licit_count,
        sum(CASE WHEN t.class = 'unknown' THEN 1 ELSE 0 END) as unknown_count
    """
    return query_to_df(query).iloc[0].to_dict()


@st.cache_data(ttl=300)
def get_relationship_stats():
    """Obtiene estadisticas de relaciones"""
    query = """
    MATCH ()-[r:TRANSFER]->()
    RETURN 
        count(r) as total_transfers,
        count(DISTINCT startNode(r)) as unique_senders,
        count(DISTINCT endNode(r)) as unique_receivers
    """
    return query_to_df(query).iloc[0].to_dict()


@st.cache_data(ttl=300)
def get_temporal_evolution():
    """Obtiene evolucion temporal de transacciones"""
    query = """
    MATCH (t:Transaction) 
    WHERE t.class IN ['1', '2']
    RETURN 
        toInteger(t.time_step) as timestep, 
        t.class as classification,
        count(t) as transaction_count
    ORDER BY timestep ASC
    """
    return query_to_df(query)


@st.cache_data(ttl=300)
def get_temporal_activity():
    """Obtiene actividad total por timestep"""
    query = """
    MATCH (t:Transaction)
    RETURN 
        toInteger(t.time_step) as timestep,
        count(t) as total_transactions,
        sum(CASE WHEN t.class = '1' THEN 1 ELSE 0 END) as illicit_count,
        sum(CASE WHEN t.class = '2' THEN 1 ELSE 0 END) as licit_count,
        sum(CASE WHEN t.class = 'unknown' THEN 1 ELSE 0 END) as unknown_count
    ORDER BY timestep
    """
    return query_to_df(query)


@st.cache_data(ttl=300)
def get_degree_distributions():
    """Obtiene top nodos por grado de entrada y salida"""
    out_query = """
    MATCH (t:Transaction)-[r:TRANSFER]->()
    WITH t, count(r) as out_degree
    RETURN 
        t.tx_id as tx_id,
        t.class as classification,
        t.time_step as timestep,
        out_degree
    ORDER BY out_degree DESC
    LIMIT 10
    """
    
    in_query = """
    MATCH ()-[r:TRANSFER]->(t:Transaction)
    WITH t, count(r) as in_degree
    RETURN 
        t.tx_id as tx_id,
        t.class as classification,
        t.time_step as timestep,
        in_degree
    ORDER BY in_degree DESC
    LIMIT 10
    """
    
    return query_to_df(out_query), query_to_df(in_query)


@st.cache_data(ttl=300)
def get_flow_patterns():
    """Obtiene patrones de flujo desde y hacia nodos ilicitos"""
    from_illicit = """
    MATCH (source:Transaction {class: '1'})-[r:TRANSFER]->(target:Transaction)
    RETURN 
        target.class as target_class,
        count(r) as transfer_count,
        count(DISTINCT source.tx_id) as unique_illicit_sources,
        count(DISTINCT target.tx_id) as unique_targets
    ORDER BY transfer_count DESC
    """
    
    to_illicit = """
    MATCH (source:Transaction)-[r:TRANSFER]->(target:Transaction {class: '1'})
    RETURN 
        source.class as source_class,
        count(r) as transfer_count,
        count(DISTINCT source.tx_id) as unique_sources,
        count(DISTINCT target.tx_id) as unique_illicit_targets
    ORDER BY transfer_count DESC
    """
    
    return query_to_df(from_illicit), query_to_df(to_illicit)


@st.cache_data(ttl=300)
def get_mixing_patterns():
    """Obtiene matriz de mixing patterns (homofilia/heterofilia)"""
    query = """
    MATCH (a:Transaction)-[r:TRANSFER]->(b:Transaction)
    WHERE a.class IN ['1', '2'] AND b.class IN ['1', '2']
    RETURN 
        a.class as source_class,
        b.class as target_class,
        count(r) as edge_count
    ORDER BY source_class, target_class
    """
    return query_to_df(query)


@st.cache_data(ttl=300)
def get_network_sample(limit=100):
    """Obtiene muestra de red para visualizacion"""
    query = f"""
    MATCH (a:Transaction)-[r:TRANSFER]->(b:Transaction)
    WHERE (a.class = '1' OR b.class = '1') 
        AND toInteger(a.time_step) >= 10 AND toInteger(a.time_step) <= 15
    RETURN 
        a.tx_id as source,
        b.tx_id as target,
        a.class as source_class,
        b.class as target_class,
        a.time_step as source_timestep,
        b.time_step as target_timestep
    LIMIT {limit}
    """
    return query_to_df(query)


# Sidebar para navegacion
st.sidebar.title("Navegacion")
page = st.sidebar.radio(
    "Seleccionar seccion:",
    ["Dashboard Principal", "Analisis Temporal", "Analisis de Red", "Patrones de Flujo", "Administracion CRUD"]
)

# Titulo principal
st.title("Bitcoin Forensics Dashboard")
st.markdown("Analisis forense del dataset Elliptic para deteccion de transacciones ilicitas")
st.markdown("---")


# PAGINA 1: DASHBOARD PRINCIPAL
if page == "Dashboard Principal":
    
    # Obtener datos
    basic_stats = get_basic_stats()
    rel_stats = get_relationship_stats()
    
    # Calculos
    total = basic_stats['total_transactions']
    illicit = basic_stats['illicit_count']
    licit = basic_stats['licit_count']
    unknown = basic_stats['unknown_count']
    total_classified = illicit + licit
    illicit_rate = (illicit / total_classified * 100) if total_classified > 0 else 0
    
    # Seccion de KPIs
    st.header("Indicadores Clave")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total de Transacciones",
            value=f"{total:,}",
            help="Numero total de transacciones en el dataset"
        )
    
    with col2:
        st.metric(
            label="Transacciones Ilicitas",
            value=f"{illicit:,}",
            delta=f"{illicit_rate:.2f}%",
            help="Transacciones clasificadas como ilicitas"
        )
    
    with col3:
        st.metric(
            label="Total de Transferencias",
            value=f"{rel_stats['total_transfers']:,}",
            help="Numero total de relaciones TRANSFER en la red"
        )
    
    with col4:
        st.metric(
            label="Rango Temporal",
            value=f"{basic_stats['min_timestep']}-{basic_stats['max_timestep']}",
            delta=f"{basic_stats['unique_timesteps']} steps",
            help="Rango de timesteps en el dataset"
        )
    
    st.markdown("---")
    
    # Fila de visualizaciones
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        st.subheader("Distribucion de Transacciones")
        
        # Grafico de pie
        labels = ['Ilicitas', 'Licitas', 'Desconocidas']
        values = [illicit, licit, unknown]
        colors = ['#FF4B4B', '#2ECC71', '#BDC3C7']
        
        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=0.3,
            marker_colors=colors,
            textinfo='label+percent+value',
            textposition='auto'
        )])
        
        fig.update_layout(
            height=400,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col_right:
        st.subheader("Metricas de Red")
        
        # Estadisticas adicionales
        avg_out_degree = rel_stats['total_transfers'] / rel_stats['unique_senders'] if rel_stats['unique_senders'] > 0 else 0
        avg_in_degree = rel_stats['total_transfers'] / rel_stats['unique_receivers'] if rel_stats['unique_receivers'] > 0 else 0
        
        metrics_data = {
            'Metrica': [
                'Nodos Emisores Unicos',
                'Nodos Receptores Unicos',
                'Promedio Out-Degree',
                'Promedio In-Degree',
                'Densidad Estimada'
            ],
            'Valor': [
                f"{rel_stats['unique_senders']:,}",
                f"{rel_stats['unique_receivers']:,}",
                f"{avg_out_degree:.2f}",
                f"{avg_in_degree:.2f}",
                f"{(rel_stats['total_transfers'] / (total * (total - 1))):.6f}"
            ]
        }
        
        df_metrics = pd.DataFrame(metrics_data)
        
        # Grafico de barras horizontal
        fig = go.Figure(go.Bar(
            y=df_metrics['Metrica'],
            x=[rel_stats['unique_senders'], rel_stats['unique_receivers'], 
               avg_out_degree, avg_in_degree, 
               rel_stats['total_transfers'] / (total * (total - 1))],
            orientation='h',
            marker=dict(color='#3498DB'),
            text=[f"{rel_stats['unique_senders']:,}", f"{rel_stats['unique_receivers']:,}",
                  f"{avg_out_degree:.2f}", f"{avg_in_degree:.2f}",
                  f"{(rel_stats['total_transfers'] / (total * (total - 1))):.6f}"],
            textposition='auto'
        ))
        
        fig.update_layout(
            height=400,
            showlegend=False,
            xaxis_title="",
            yaxis_title=""
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Resumen estadistico
    st.subheader("Resumen Estadistico")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Clasificacion**")
        st.write(f"- Ilicitas: {illicit:,} ({(illicit/total*100):.2f}%)")
        st.write(f"- Licitas: {licit:,} ({(licit/total*100):.2f}%)")
        st.write(f"- Desconocidas: {unknown:,} ({(unknown/total*100):.2f}%)")
        st.write(f"- Ratio Ilicito/Licito: 1:{(licit/illicit):.2f}")
    
    with col2:
        st.markdown("**Actividad de Red**")
        st.write(f"- Total Transferencias: {rel_stats['total_transfers']:,}")
        st.write(f"- Nodos Activos (Emisores): {rel_stats['unique_senders']:,}")
        st.write(f"- Nodos Activos (Receptores): {rel_stats['unique_receivers']:,}")
        st.write(f"- Promedio Conexiones: {((avg_out_degree + avg_in_degree)/2):.2f}")
    
    with col3:
        st.markdown("**Temporalidad**")
        st.write(f"- Time Steps Unicos: {basic_stats['unique_timesteps']}")
        st.write(f"- Rango: {basic_stats['min_timestep']} - {basic_stats['max_timestep']}")
        st.write(f"- Duracion: {basic_stats['max_timestep'] - basic_stats['min_timestep']} steps")
        avg_tx_per_step = total / basic_stats['unique_timesteps']
        st.write(f"- Promedio TX/Step: {avg_tx_per_step:.0f}")


# PAGINA 2: ANALISIS TEMPORAL
elif page == "Analisis Temporal":
    
    st.header("Analisis Temporal de Transacciones")
    
    # Obtener datos
    temporal_df = get_temporal_evolution()
    activity_df = get_temporal_activity()
    
    if not temporal_df.empty and not activity_df.empty:
        
        # Calcular proporcion de ilicitas
        activity_df['illicit_proportion'] = (
            activity_df['illicit_count'] / 
            (activity_df['illicit_count'] + activity_df['licit_count']) * 100
        )
        
        # Graficos
        st.subheader("Evolucion Temporal: Ilicitas vs Licitas")
        
        # Separar datos
        illicit_data = temporal_df[temporal_df['classification'] == '1']
        licit_data = temporal_df[temporal_df['classification'] == '2']
        
        # Crear grafico de lineas
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=illicit_data['timestep'],
            y=illicit_data['transaction_count'],
            mode='lines+markers',
            name='Ilicitas',
            line=dict(color='#FF4B4B', width=3),
            marker=dict(size=6)
        ))
        
        fig.add_trace(go.Scatter(
            x=licit_data['timestep'],
            y=licit_data['transaction_count'],
            mode='lines+markers',
            name='Licitas',
            line=dict(color='#2ECC71', width=3),
            marker=dict(size=6)
        ))
        
        fig.update_layout(
            height=500,
            xaxis_title="Timestep",
            yaxis_title="Numero de Transacciones",
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Actividad total y proporcion
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Actividad Total por Timestep")
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=activity_df['timestep'],
                y=activity_df['total_transactions'],
                mode='lines',
                name='Total',
                line=dict(color='#3498DB', width=2),
                fill='tozeroy',
                fillcolor='rgba(52, 152, 219, 0.3)'
            ))
            
            fig.update_layout(
                height=400,
                xaxis_title="Timestep",
                yaxis_title="Transacciones Totales",
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Proporcion de Transacciones Ilicitas")
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=activity_df['timestep'],
                y=activity_df['illicit_proportion'],
                mode='lines+markers',
                name='% Ilicitas',
                line=dict(color='#E74C3C', width=3),
                marker=dict(size=6)
            ))
            
            # Linea de referencia
            avg_proportion = activity_df['illicit_proportion'].mean()
            fig.add_hline(
                y=avg_proportion,
                line_dash="dash",
                line_color="gray",
                annotation_text=f"Promedio: {avg_proportion:.2f}%"
            )
            
            fig.update_layout(
                height=400,
                xaxis_title="Timestep",
                yaxis_title="Porcentaje de Ilicitas (%)",
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Estadisticas temporales
        st.markdown("---")
        st.subheader("Estadisticas Temporales")
        
        col1, col2, col3, col4 = st.columns(4)
        
        # Top timesteps con mayor actividad
        top_activity = activity_df.nlargest(5, 'total_transactions')
        top_illicit = activity_df.nlargest(5, 'illicit_count')
        
        with col1:
            st.markdown("**Promedio de Proporcion Ilicita**")
            st.metric("", f"{activity_df['illicit_proportion'].mean():.2f}%")
        
        with col2:
            st.markdown("**Maximo de Proporcion Ilicita**")
            max_prop_idx = activity_df['illicit_proportion'].idxmax()
            max_prop = activity_df.loc[max_prop_idx]
            st.metric("", f"{max_prop['illicit_proportion']:.2f}%", 
                     delta=f"Timestep {max_prop['timestep']}")
        
        with col3:
            st.markdown("**Minimo de Proporcion Ilicita**")
            min_prop_idx = activity_df['illicit_proportion'].idxmin()
            min_prop = activity_df.loc[min_prop_idx]
            st.metric("", f"{min_prop['illicit_proportion']:.2f}%",
                     delta=f"Timestep {min_prop['timestep']}")
        
        with col4:
            st.markdown("**Variabilidad (Desv. Std)**")
            st.metric("", f"{activity_df['illicit_proportion'].std():.2f}%")
        
        # Tablas de top timesteps
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Top 5 Timesteps con Mayor Actividad**")
            display_df = top_activity[['timestep', 'total_transactions', 'illicit_count', 'licit_count']].copy()
            display_df.columns = ['Timestep', 'Total', 'Ilicitas', 'Licitas']
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        with col2:
            st.markdown("**Top 5 Timesteps con Mayor Actividad Ilicita**")
            display_df = top_illicit[['timestep', 'illicit_count', 'total_transactions', 'illicit_proportion']].copy()
            display_df.columns = ['Timestep', 'Ilicitas', 'Total', '% Ilicitas']
            display_df['% Ilicitas'] = display_df['% Ilicitas'].apply(lambda x: f"{x:.2f}%")
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    else:
        st.warning("No hay datos temporales disponibles")


# PAGINA 3: ANALISIS DE RED
elif page == "Analisis de Red":
    
    st.header("Analisis de Red y Conectividad")
    
    # Obtener datos
    top_out_df, top_in_df = get_degree_distributions()
    network_df = get_network_sample(100)
    
    # Top nodos por centralidad
    st.subheader("Nodos con Mayor Centralidad")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Top 10 Nodos por Out-Degree (Emisores)**")
        
        if not top_out_df.empty:
            # Crear una copia para display
            display_df = top_out_df.copy()
            display_df['classification'] = display_df['classification'].map({
                '1': 'Ilicita',
                '2': 'Licita',
                'unknown': 'Desconocida'
            })
            display_df = display_df[['tx_id', 'classification', 'out_degree', 'timestep']]
            display_df.columns = ['ID Transaccion', 'Clase', 'Out-Degree', 'Timestep']
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
        else:
            st.info("No hay datos disponibles")
    
    with col2:
        st.markdown("**Top 10 Nodos por In-Degree (Receptores)**")
        
        if not top_in_df.empty:
            # Crear una copia para display
            display_df = top_in_df.copy()
            display_df['classification'] = display_df['classification'].map({
                '1': 'Ilicita',
                '2': 'Licita',
                'unknown': 'Desconocida'
            })
            display_df = display_df[['tx_id', 'classification', 'in_degree', 'timestep']]
            display_df.columns = ['ID Transaccion', 'Clase', 'In-Degree', 'Timestep']
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
           
        else:
            st.info("No hay datos disponibles")
    
    # Visualizacion de red
    st.markdown("---")
    st.subheader("Visualizacion de Red (Subgrafo Ilicito)")
    
    if not network_df.empty and len(network_df) > 0:
        
        # Crear grafo con NetworkX
        G = nx.from_pandas_edgelist(network_df, source='source', target='target', create_using=nx.DiGraph())
        
        # Layout
        pos = nx.spring_layout(G, seed=42, k=0.5, iterations=50)
        
        # Preparar datos de aristas
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
            mode='lines',
            name='Transferencias'
        )
        
        # Preparar datos de nodos
        node_info = {}
        for _, row in network_df.iterrows():
            node_info[row['source']] = {
                'class': row['source_class'],
                'timestep': row['source_timestep']
            }
            node_info[row['target']] = {
                'class': row['target_class'],
                'timestep': row['target_timestep']
            }
        
        node_x = []
        node_y = []
        node_text = []
        node_color = []
        node_size = []
        
        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            
            node_class = node_info.get(node, {}).get('class', 'unknown')
            node_timestep = node_info.get(node, {}).get('timestep', 'N/A')
            degree = G.degree(node)
            
            if node_class == '1':
                color = '#FF4B4B'
                class_name = 'Ilicito'
            elif node_class == '2':
                color = '#2ECC71'
                class_name = 'Licito'
            else:
                color = '#BDC3C7'
                class_name = 'Desconocido'
            
            node_color.append(color)
            node_size.append(max(10, min(25, degree * 3)))
            
            node_text.append(
                f"ID: {node}<br>"
                f"Clase: {class_name}<br>"
                f"Timestep: {node_timestep}<br>"
                f"Grado: {degree}"
            )
        
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers',
            hoverinfo='text',
            text=node_text,
            marker=dict(
                showscale=False,
                color=node_color,
                size=node_size,
                line_width=1,
                line_color='black'
            ),
            name='Transacciones'
        )
        
        # Crear figura
        fig = go.Figure(
            data=[edge_trace, node_trace],
            layout=go.Layout(
                title=dict(
                    text='Red de Transacciones Bitcoin (Timesteps 10-15)',
                    font=dict(size=16)
                ),
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20, l=5, r=5, t=60),
                annotations=[dict(
                    text="Rojo: Ilicito | Verde: Licito | Gris: Desconocido",
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.5, y=-0.05,
                    xanchor='center', yanchor='bottom',
                    font=dict(size=12)
                )],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                plot_bgcolor='white',
                height=600
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Metricas del subgrafo
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Nodos", G.number_of_nodes())
        
        with col2:
            st.metric("Aristas", G.number_of_edges())
        
        with col3:
            st.metric("Densidad", f"{nx.density(G):.4f}")
        
        with col4:
            st.metric("Componentes Debiles", nx.number_weakly_connected_components(G))
        
    else:
        st.warning("No hay datos de red disponibles")


# PAGINA 4: PATRONES DE FLUJO
elif page == "Patrones de Flujo":
    
    st.header("Analisis de Patrones de Flujo de Fondos")
    
    # Obtener datos
    from_illicit_df, to_illicit_df = get_flow_patterns()
    mixing_df = get_mixing_patterns()
    
    # Flujos desde y hacia nodos ilicitos
    st.subheader("Flujos de Fondos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Flujos desde Nodos Ilicitos**")
        
        if not from_illicit_df.empty:
            # Preparar datos
            display_df = from_illicit_df.copy()
            display_df['target_class'] = display_df['target_class'].map({
                '1': 'Ilicitos',
                '2': 'Licitos',
                'unknown': 'Desconocidos'
            })
            display_df.columns = ['Destino', 'Transferencias', 'Fuentes Ilicitas', 'Destinos Unicos']
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Grafico
            colors_map = {'Ilicitos': '#FF4B4B', 'Licitos': '#2ECC71', 'Desconocidos': '#BDC3C7'}
            colors = [colors_map.get(x, '#BDC3C7') for x in display_df['Destino']]
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                x=display_df['Destino'],
                y=display_df['Transferencias'],
                marker=dict(color=colors),
                text=display_df['Transferencias'],
                textposition='auto'
            ))
            
            fig.update_layout(
                height=350,
                xaxis_title="Tipo de Destino",
                yaxis_title="Numero de Transferencias",
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos disponibles")
    
    with col2:
        st.markdown("**Flujos hacia Nodos Ilicitos**")
        
        if not to_illicit_df.empty:
            # Preparar datos
            display_df = to_illicit_df.copy()
            display_df['source_class'] = display_df['source_class'].map({
                '1': 'Ilicitos',
                '2': 'Licitos',
                'unknown': 'Desconocidos'
            })
            display_df.columns = ['Origen', 'Transferencias', 'Fuentes Unicas', 'Destinos Ilicitos']
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Grafico
            colors_map = {'Ilicitos': '#FF4B4B', 'Licitos': '#2ECC71', 'Desconocidos': '#BDC3C7'}
            colors = [colors_map.get(x, '#BDC3C7') for x in display_df['Origen']]
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                x=display_df['Origen'],
                y=display_df['Transferencias'],
                marker=dict(color=colors),
                text=display_df['Transferencias'],
                textposition='auto'
            ))
            
            fig.update_layout(
                height=350,
                xaxis_title="Tipo de Origen",
                yaxis_title="Numero de Transferencias",
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay datos disponibles")
    
    # Matriz de Mixing Patterns
    st.markdown("---")
    st.subheader("Matriz de Mixing Patterns (Homofilia vs Heterofilia)")
    
    if not mixing_df.empty:
        
        # Calcular coeficiente de homofilia
        total_edges = mixing_df['edge_count'].sum()
        homophilic_edges = mixing_df[mixing_df['source_class'] == mixing_df['target_class']]['edge_count'].sum()
        homophily_coefficient = homophilic_edges / total_edges if total_edges > 0 else 0
        
        # Mostrar coeficiente
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            st.metric(
                "Coeficiente de Homofilia",
                f"{homophily_coefficient:.3f}",
                delta=f"{homophily_coefficient * 100:.1f}% conexiones homofilicas",
                help="Proporcion de conexiones entre nodos del mismo tipo"
            )
        
        # Crear matriz pivot
        mixing_pivot = mixing_df.pivot(
            index='source_class',
            columns='target_class',
            values='edge_count'
        ).fillna(0)
        
        # Renombrar indices
        mixing_pivot.index = mixing_pivot.index.map({'1': 'Ilicito', '2': 'Licito'})
        mixing_pivot.columns = mixing_pivot.columns.map({'1': 'Ilicito', '2': 'Licito'})
        
        # Heatmap
        fig = go.Figure()
        
        fig.add_trace(go.Heatmap(
            z=mixing_pivot.values,
            x=mixing_pivot.columns,
            y=mixing_pivot.index,
            colorscale='YlOrRd',
            text=mixing_pivot.values,
            texttemplate='%{text:,.0f}',
            textfont={"size": 14},
            colorbar=dict(title="Transferencias")
        ))
        
        fig.update_layout(
            title="Matriz de Mixing Patterns<br>(Filas: Fuente, Columnas: Destino)",
            xaxis_title="Clase del Nodo Destino",
            yaxis_title="Clase del Nodo Fuente",
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabla de detalle
        st.markdown("**Detalle de Mixing Patterns**")
        
        detail_data = []
        for _, row in mixing_df.iterrows():
            source_label = "Ilicito" if row['source_class'] == '1' else "Licito"
            target_label = "Ilicito" if row['target_class'] == '1' else "Licito"
            pattern_type = "Homofilia" if row['source_class'] == row['target_class'] else "Heterofilia"
            percentage = (row['edge_count'] / total_edges * 100)
            
            detail_data.append({
                'Patron': f"{source_label} → {target_label}",
                'Tipo': pattern_type,
                'Transferencias': row['edge_count'],
                'Porcentaje': f"{percentage:.2f}%"
            })
        
        detail_df = pd.DataFrame(detail_data)
        st.dataframe(detail_df, use_container_width=True, hide_index=True)
        
        # Interpretacion
        st.markdown("---")
        st.markdown("**Interpretacion**")
        st.write(f"- **Homofilia**: {homophily_coefficient * 100:.1f}% de las conexiones son entre nodos del mismo tipo (Ilicito-Ilicito o Licito-Licito)")
        st.write(f"- **Heterofilia**: {(1 - homophily_coefficient) * 100:.1f}% de las conexiones son entre nodos de diferentes tipos (Ilicito-Licito o Licito-Ilicito)")
        
        if homophily_coefficient > 0.5:
            st.info("El alto coeficiente de homofilia indica que existe una tendencia significativa a que las transacciones ilicitas se conecten entre si, formando comunidades separadas de las transacciones licitas.")
        else:
            st.info("El bajo coeficiente de homofilia indica que existe una mezcla considerable entre transacciones ilicitas y licitas, lo que puede dificultar la deteccion de patrones.")
    
    else:
        st.warning("No hay datos de mixing patterns disponibles")


# PAGINA 5: ADMINISTRACION CRUD
elif page == "Administracion CRUD":
    
    st.header("Administracion de Transacciones")
    st.markdown("Sistema de gestion CRUD para transacciones en la base de datos")
    
    # Selector de operacion
    operation = st.selectbox(
        "Seleccionar operacion:",
        ["Crear", "Leer", "Actualizar", "Eliminar"]
    )
    
    st.markdown("---")
    
    # CREAR
    if operation == "Crear":
        st.subheader("Crear Nueva Transaccion")
        
        with st.form("create_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                tx_id = st.text_input("ID de Transaccion (tx_id)*", help="Identificador unico de la transaccion")
                time_step = st.number_input("Time Step*", min_value=1, step=1, help="Paso temporal de la transaccion")
            
            with col2:
                clase = st.selectbox("Clasificacion (class)*", ['1', '2', 'unknown'], 
                                    help="1 = Ilicita, 2 = Licita, unknown = Desconocida")
                features_input = st.text_area(
                    "Features (opcional)",
                    help="Lista de features separadas por comas. Ejemplo: 0.1, -0.23, 1.0"
                )
            
            submitted = st.form_submit_button("Crear Transaccion")
            
            if submitted:
                if not tx_id:
                    st.error("El ID de transaccion es obligatorio")
                else:
                    try:
                        params = {
                            "tx_id": str(tx_id),
                            "time_step": int(time_step),
                            "clase": str(clase)
                        }
                        
                        if features_input and features_input.strip():
                            feats = [float(x.strip()) for x in features_input.split(",") if x.strip()]
                            params["features"] = feats
                            query = """
                            CREATE (t:Transaction {
                                tx_id: $tx_id,
                                time_step: $time_step,
                                class: $clase,
                                features: $features
                            })
                            """
                        else:
                            query = """
                            CREATE (t:Transaction {
                                tx_id: $tx_id,
                                time_step: $time_step,
                                class: $clase
                            })
                            """
                        
                        run_query(query, params)
                        get_basic_stats.clear()
                        st.success(f"Transaccion {tx_id} creada exitosamente")
                        
                    except Exception as e:
                        st.error(f"Error al crear transaccion: {str(e)}")
    
    # LEER
    elif operation == "Leer":
        st.subheader("Consultar Transacciones")
        
        # Opciones de busqueda
        search_type = st.radio("Tipo de busqueda:", ["Por ID", "Listar todas (limitado a 100)"])
        
        if search_type == "Por ID":
            # Obtener lista de IDs
            try:
                ids = run_query("MATCH (t:Transaction) RETURN t.tx_id AS id LIMIT 500")
                id_list = [row["id"] for row in ids if row.get("id")]
                
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    choice = st.selectbox(
                        "Seleccionar ID o ingresar manualmente:",
                        ["-- Ingresar manualmente --"] + id_list
                    )
                
                if choice == "-- Ingresar manualmente --":
                    selected_id = st.text_input("Escribir tx_id:")
                else:
                    selected_id = choice
                
                with col2:
                    st.write("")
                    st.write("")
                    search_btn = st.button("Buscar", use_container_width=True)
                
                if search_btn:
                    if not selected_id:
                        st.warning("Ingresa o selecciona un tx_id")
                    else:
                        try:
                            result = run_query(
                                "MATCH (t:Transaction {tx_id: $id}) RETURN t",
                                {"id": selected_id}
                            )
                            
                            if result:
                                st.success("Transaccion encontrada")
                                
                                tx_data = result[0]["t"]
                                
                                # Mostrar en formato organizado
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.markdown("**Informacion Basica**")
                                    st.write(f"- ID: {tx_data.get('tx_id', 'N/A')}")
                                    st.write(f"- Time Step: {tx_data.get('time_step', 'N/A')}")
                                    
                                    clase = tx_data.get('class', 'N/A')
                                    clase_label = "Ilicita" if clase == '1' else ("Licita" if clase == '2' else "Desconocida")
                                    st.write(f"- Clasificacion: {clase_label} ({clase})")
                                
                                with col2:
                                    st.markdown("**Informacion Adicional**")
                                    if 'features' in tx_data and tx_data['features']:
                                        st.write(f"- Features: {len(tx_data['features'])} valores")
                                    else:
                                        st.write("- Features: No disponibles")
                                
                                # Mostrar JSON completo
                                with st.expander("Ver JSON completo"):
                                    st.json(tx_data)
                                
                            else:
                                st.info("No se encontro ninguna transaccion con ese ID")
                                
                        except Exception as e:
                            st.error(f"Error al buscar: {str(e)}")
            
            except Exception as e:
                st.error(f"Error al obtener lista de IDs: {str(e)}")
        
        else:
            # Listar todas
            try:
                query = """
                MATCH (t:Transaction)
                RETURN t.tx_id as tx_id, t.time_step as time_step, t.class as class
                ORDER BY t.time_step DESC
                LIMIT 100
                """
                
                results = query_to_df(query)
                
                if not results.empty:
                    # Mapear clasificaciones
                    results['class'] = results['class'].map({
                        '1': 'Ilicita',
                        '2': 'Licita',
                        'unknown': 'Desconocida'
                    })
                    
                    results.columns = ['ID Transaccion', 'Time Step', 'Clasificacion']
                    
                    st.dataframe(results, use_container_width=True, hide_index=True)
                    st.caption(f"Mostrando {len(results)} transacciones")
                else:
                    st.info("No hay transacciones en la base de datos")
                    
            except Exception as e:
                st.error(f"Error al listar transacciones: {str(e)}")
    
    # ACTUALIZAR
    elif operation == "Actualizar":
        st.subheader("Actualizar Transaccion Existente")
        
        with st.form("update_form"):
            tx_id_update = st.text_input(
                "ID de Transaccion a actualizar*",
                help="Identificador de la transaccion a modificar"
            )
            
            nueva_clase = st.selectbox(
                "Nueva clasificacion*",
                ['1', '2', 'unknown'],
                help="1 = Ilicita, 2 = Licita, unknown = Desconocida"
            )
            
            nuevo_timestep = st.number_input(
                "Nuevo Time Step (opcional)",
                min_value=0,
                value=0,
                help="Dejar en 0 para no modificar"
            )
            
            submitted = st.form_submit_button("Actualizar Transaccion")
            
            if submitted:
                if not tx_id_update:
                    st.error("El ID de transaccion es obligatorio")
                else:
                    try:
                        # Verificar si existe
                        check = run_query(
                            "MATCH (t:Transaction {tx_id: $id}) RETURN count(t) as count",
                            {"id": tx_id_update}
                        )
                        
                        if check[0]['count'] == 0:
                            st.error(f"No existe ninguna transaccion con ID: {tx_id_update}")
                        else:
                            # Actualizar
                            if nuevo_timestep > 0:
                                query = """
                                MATCH (t:Transaction {tx_id: $id})
                                SET t.class = $nueva_clase, t.time_step = $timestep
                                """
                                params = {
                                    "id": tx_id_update,
                                    "nueva_clase": nueva_clase,
                                    "timestep": nuevo_timestep
                                }
                            else:
                                query = """
                                MATCH (t:Transaction {tx_id: $id})
                                SET t.class = $nueva_clase
                                """
                                params = {
                                    "id": tx_id_update,
                                    "nueva_clase": nueva_clase
                                }
                            
                            run_query(query, params)
                            get_basic_stats.clear()
                            st.success(f"Transaccion {tx_id_update} actualizada exitosamente")
                            
                    except Exception as e:
                        st.error(f"Error al actualizar: {str(e)}")
    
    # ELIMINAR
    elif operation == "Eliminar":
        st.subheader("Eliminar Transaccion")
        
        st.warning("Esta operacion es irreversible. La transaccion y sus relaciones seran eliminadas permanentemente.")
        
        with st.form("delete_form"):
            tx_id_delete = st.text_input(
                "ID de Transaccion a eliminar*",
                help="Identificador de la transaccion a eliminar"
            )
            
            confirm = st.checkbox("Confirmo que deseo eliminar esta transaccion")
            
            submitted = st.form_submit_button("Eliminar Transaccion", type="primary")
            
            if submitted:
                if not tx_id_delete:
                    st.error("El ID de transaccion es obligatorio")
                elif not confirm:
                    st.error("Debes confirmar la eliminacion")
                else:
                    try:
                        # Verificar si existe
                        check = run_query(
                            "MATCH (t:Transaction {tx_id: $id}) RETURN count(t) as count",
                            {"id": tx_id_delete}
                        )
                        
                        if check[0]['count'] == 0:
                            st.error(f"No existe ninguna transaccion con ID: {tx_id_delete}")
                        else:
                            # Eliminar
                            query = "MATCH (t:Transaction {tx_id: $id}) DETACH DELETE t"
                            run_query(query, {"id": tx_id_delete})
                            get_basic_stats.clear()
                            st.success(f"Transaccion {tx_id_delete} eliminada exitosamente")
                            
                    except Exception as e:
                        st.error(f"Error al eliminar: {str(e)}")


# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
    <small>Bitcoin Forensics Dashboard | Dataset Elliptic | Neo4j AuraDB</small>
    </div>
    """,
    unsafe_allow_html=True
)
