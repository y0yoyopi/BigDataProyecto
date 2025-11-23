# Bitcoin Forensics: Analisis de Transacciones Ilicitas

Proyecto de analisis forense de transacciones Bitcoin utilizando el dataset Elliptic, implementado con Neo4j Graph Database y visualizaciones interactivas en Streamlit.

## Descripcion del Proyecto

Este proyecto implementa un sistema completo de analisis forense para la deteccion de actividades ilicitas en la red Bitcoin. Utilizando el dataset Elliptic, que contiene transacciones reales clasificadas, se desarrolla un dashboard interactivo y un sistema de analisis exploratorio basado en teoria de grafos y analisis de redes.

### Objetivos

- Analizar patrones de transacciones ilicitas en la blockchain de Bitcoin
- Identificar nodos sospechosos mediante analisis de centralidad
- Visualizar la estructura de la red y flujos de fondos
- Detectar patrones de homofilia y heterofilia en transacciones
- Proporcionar herramientas interactivas para exploracion de datos

## Dataset

**Elliptic Bitcoin Dataset**
- 203,769 transacciones de Bitcoin
- 234,355 transferencias (relaciones)
- 49 timesteps temporales (aproximadamente 49 semanas)
- 166 features por transaccion

**Clasificaciones:**
- Clase 1: Transacciones ilicitas (actividades ilegales confirmadas)
- Clase 2: Transacciones licitas (actividades legitimas)
- Clase unknown: Transacciones sin clasificar

**Fuente:** [Elliptic Dataset](https://www.kaggle.com/datasets/ellipticco/elliptic-data-set)

## Arquitectura

### Tecnologias Utilizadas

**Base de Datos:**
- Neo4j AuraDB (Graph Database en la nube)
- Cypher Query Language

**Backend:**
- Python 3.11+
- Neo4j Driver 5.x

**Frontend y Visualizacion:**
- Streamlit (dashboard interactivo)
- Plotly (graficos interactivos)
- Matplotlib/Seaborn (visualizaciones estaticas)
- NetworkX (analisis de redes)

**Analisis de Datos:**
- Pandas (manipulacion de datos)
- NumPy (computacion numerica)
- Jupyter Notebooks (analisis exploratorio)

### Estructura del Proyecto

```
PROYECTO/
├── data/
│   ├── raw/                      # Datos crudos del dataset Elliptic
│   │   ├── elliptic_txs_classes.csv
│   │   ├── elliptic_txs_edgelist.csv
│   │   └── elliptic_txs_features.csv
│   └── processed/                # Datos procesados para Neo4j
│       ├── nodes.csv
│       └── rels.csv
├── etl/
│   └── prepare_elliptic.py      # Script de procesamiento ETL
├── db/
│   ├── app.py                   # Dashboard original
│   ├── app2.py                  # Dashboard mejorado (principal)
│   ├── explore.ipynb            # Notebook de analisis exploratorio
│   ├── neo4j_utils.py           # Utilidades de conexion
│   └── README_DASHBOARD.md      # Documentacion del dashboard
├── neo4j/
│   ├── import/                  # Archivos CSV para importacion
│   ├── data/                    # Datos de la base de datos
│   └── plugins/                 # Plugins de Neo4j
├── requirements.txt             # Dependencias de Python
└── docker-compose.yml           # Configuracion de Docker
```

## Instalacion y Configuracion

### Requisitos Previos

- Python 3.11 o superior
- Cuenta en Neo4j AuraDB (o instalacion local de Neo4j)
- pip (gestor de paquetes de Python)

### Paso 1: Clonar el Repositorio

```bash
git clone https://github.com/y0yoyopi/BigDataProyecto.git
cd BigDataProyecto
```

### Paso 2: Crear Entorno Virtual

```bash
python -m venv venv_streamlit

# En macOS/Linux
source venv_streamlit/bin/activate

# En Windows
venv_streamlit\Scripts\activate
```

### Paso 3: Instalar Dependencias

```bash
pip install -r requirements.txt
```

### Paso 4: Configurar Neo4j

1. Crear una instancia en [Neo4j AuraDB](https://neo4j.com/cloud/aura/) (gratuita)
2. Obtener las credenciales (URI y password)
3. Actualizar las credenciales en `db/app2.py`:

```python
URI = "neo4j+ssc://your-instance.databases.neo4j.io"
AUTH = ("neo4j", "your-password")
```

### Paso 5: Procesar y Cargar Datos

```bash
# Procesar datos crudos
python etl/prepare_elliptic.py

# Importar a Neo4j (desde Neo4j Browser o usando el script)
# Ver instrucciones en db/README_DASHBOARD.md
```

## Uso

### Dashboard Interactivo

Ejecutar el dashboard principal:

```bash
cd db
streamlit run app2.py
```

El dashboard estara disponible en `http://localhost:8501`

### Analisis Exploratorio

Abrir el notebook de Jupyter:

```bash
cd db
jupyter notebook explore.ipynb
```

## Funcionalidades

### 1. Dashboard Principal

**Indicadores Clave (KPIs):**
- Total de transacciones en el dataset
- Numero y porcentaje de transacciones ilicitas
- Total de transferencias en la red
- Rango temporal del dataset

**Visualizaciones:**
- Distribucion de transacciones por clasificacion
- Metricas de red (grados promedio, densidad)
- Resumen estadistico completo

### 2. Analisis Temporal

- Evolucion temporal de transacciones ilicitas vs licitas
- Actividad total por timestep
- Proporcion de transacciones ilicitas en el tiempo
- Identificacion de picos de actividad sospechosa
- Top timesteps por volumen de fraude

### 3. Analisis de Red

**Centralidad:**
- Top 10 nodos por out-degree (principales emisores)
- Top 10 nodos por in-degree (principales receptores)
- Identificacion de hubs en la red

**Visualizacion:**
- Grafo interactivo de subred ilicita
- Nodos coloreados por clasificacion
- Tamaño proporcional al grado de conexion
- Metricas del subgrafo (densidad, componentes)

### 4. Patrones de Flujo

**Analisis de Flujos:**
- Flujos desde nodos ilicitos (destinos del dinero ilegal)
- Flujos hacia nodos ilicitos (origenes de fondos)
- Matriz de mixing patterns

**Metricas Avanzadas:**
- Coeficiente de homofilia (tendencia de conexion entre nodos similares)
- Analisis de heterofilia (conexiones entre tipos diferentes)
- Heatmap de patrones de transferencia

### 5. Administracion CRUD

Sistema completo de gestion de transacciones:
- **Crear:** Agregar nuevas transacciones con features opcionales
- **Leer:** Consultar por ID o listar todas las transacciones
- **Actualizar:** Modificar clasificacion y timestep
- **Eliminar:** Remover transacciones con confirmacion

## Hallazgos Principales

### Estructura de Red

1. **Red Scale-Free:** La distribucion de grados sigue una ley de potencia, indicando presencia de hubs criticos
2. **Hubs Ilicitos:** Nodos con alta centralidad frecuentemente involucrados en actividades ilicitas
3. **Densidad Baja:** Red dispersa con clusters locales densos

### Patrones Temporales

1. **Variabilidad Temporal:** Actividad ilicita fluctua significativamente entre timesteps
2. **Picos de Actividad:** Periodos especificos con concentracion de transacciones sospechosas
3. **Tendencias:** Proporcion de ilicitas relativamente estable con anomalias detectables

### Flujos de Fondos

1. **Homofilia Significativa:** Tendencia de transacciones ilicitas a conectarse entre si
2. **Lavado de Dinero:** Patrones detectables de flujo desde ilicitos hacia licitos
3. **Mixing Patterns:** Matriz de transferencias revela estrategias de ocultamiento

## Casos de Uso

### Investigacion Forense

- Rastreo de flujos de fondos sospechosos
- Identificacion de redes criminales
- Analisis de patrones de lavado de dinero

### Compliance y Regulacion

- Monitoreo de transacciones de alto riesgo
- Evaluacion de riesgo de contrapartes
- Reportes de actividades sospechosas

### Investigacion Academica

- Estudio de redes criminales en blockchain
- Analisis de teoria de grafos aplicada
- Desarrollo de algoritmos de deteccion

### Machine Learning

- Entrenamiento de modelos de clasificacion
- Prediccion de transacciones desconocidas
- Deteccion de anomalias en tiempo real

## Metricas y Algoritmos

### Centralidad

- **Degree Centrality:** Numero de conexiones directas
- **In-Degree:** Transacciones recibidas
- **Out-Degree:** Transacciones enviadas

### Teoria de Grafos

- **Densidad:** Proporcion de conexiones reales vs posibles
- **Componentes Conectados:** Subgrafos aislados
- **Coeficiente de Clustering:** Tendencia a formar triangulos

### Analisis de Patrones

- **Homofilia:** Proporcion de conexiones intra-clase
- **Asortatividad:** Correlacion de propiedades entre nodos conectados
- **Mixing Matrix:** Distribucion de conexiones entre clases

## Limitaciones

1. **Memoria de Neo4j AuraDB:** Plan gratuito limitado a 250 MiB
2. **Timesteps Discretos:** Granularidad temporal limitada (49 semanas)
3. **Features Anonimizadas:** 166 features sin descripcion semantica
4. **Dataset Estatico:** No incluye transacciones posteriores a la recoleccion

## Trabajo Futuro

### Machine Learning

- Implementar Random Forest para clasificacion
- Graph Neural Networks (GNN) para deteccion
- Transfer Learning con modelos pre-entrenados

### Analisis Avanzado

- Deteccion de comunidades (Louvain, Label Propagation)
- Analisis de series temporales
- Prediccion de actividad futura

### Visualizacion

- Grafos 3D interactivos
- Animaciones temporales
- Dashboards en tiempo real

### Escalabilidad

- Migracion a Neo4j Enterprise
- Procesamiento distribuido con Apache Spark
- APIs REST para integracion

## Contribuciones

Las contribuciones son bienvenidas. Por favor:

1. Fork el repositorio
2. Crear una rama para la feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit de cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear un Pull Request

## Licencia

Este proyecto es de codigo abierto y esta disponible bajo la Licencia MIT.

## Referencias

- Elliptic Dataset: [Kaggle](https://www.kaggle.com/datasets/ellipticco/elliptic-data-set)
- Paper original: Weber et al. (2019) "Anti-Money Laundering in Bitcoin: Experimenting with Graph Convolutional Networks for Financial Forensics"
- Neo4j Documentation: [https://neo4j.com/docs/](https://neo4j.com/docs/)
- Streamlit Documentation: [https://docs.streamlit.io/](https://docs.streamlit.io/)

## Contacto

Para preguntas, sugerencias o colaboraciones:

- GitHub: [@y0yoyopi](https://github.com/y0yoyopi)
- Repositorio: [BigDataProyecto](https://github.com/y0yoyopi/BigDataProyecto)

## Agradecimientos

- Elliptic por proporcionar el dataset
- Neo4j por la plataforma AuraDB
- Comunidad de Streamlit por las herramientas de visualizacion
- Contribuidores de NetworkX y otras librerias open source

---

**Nota:** Este proyecto fue desarrollado con fines educativos y de investigacion. Los datos utilizados son publicos y anonimizados.