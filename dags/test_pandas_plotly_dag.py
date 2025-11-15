from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'test_pandas_plotly_dag',
    default_args=default_args,
    description='Test pandas and plotly functionality',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'pandas', 'plotly']
)

def test_pandas():
    import pandas as pd
    import numpy as np
    
    # Create sample data
    data = {
        'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
        'age': [25, 30, 35, 28],
        'salary': [50000, 60000, 70000, 55000]
    }
    
    df = pd.DataFrame(data)
    print("DataFrame created:")
    print(df)
    
    # Basic operations
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Average age: {df['age'].mean()}")
    print(f"Total salary: {df['salary'].sum()}")
    
    return "Pandas test completed successfully"

def test_plotly():
    import plotly.graph_objects as go
    import plotly.express as px
    import pandas as pd
    
    # Create sample data
    df = pd.DataFrame({
        'x': [1, 2, 3, 4, 5],
        'y': [2, 4, 3, 5, 6],
        'category': ['A', 'B', 'A', 'B', 'A']
    })
    
    # Create a simple plot
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['x'], y=df['y'], mode='lines+markers'))
    fig.update_layout(title='Test Plot', xaxis_title='X', yaxis_title='Y')
    
    print("Plotly figure created successfully")
    print(f"Figure data: {len(fig.data)} traces")
    
    # Test plotly express
    fig2 = px.scatter(df, x='x', y='y', color='category', title='Scatter Plot')
    print("Plotly Express scatter plot created")
    
    return "Plotly test completed successfully"

def test_combined():
    import pandas as pd
    import plotly.express as px
    import numpy as np
    
    # Generate sample time series data
    dates = pd.date_range('2025-01-01', periods=30, freq='D')
    values = np.random.randn(30).cumsum()
    
    df = pd.DataFrame({
        'date': dates,
        'value': values,
        'category': np.random.choice(['Type A', 'Type B'], 30)
    })
    
    print("Combined test - DataFrame:")
    print(df.head())
    
    # Create visualization
    fig = px.line(df, x='date', y='value', color='category', 
                  title='Time Series with Pandas and Plotly')
    
    print(f"Combined test completed - {len(df)} data points processed")
    return "Combined pandas + plotly test successful"

# Define tasks
pandas_task = PythonOperator(
    task_id='test_pandas',
    python_callable=test_pandas,
    dag=dag
)

plotly_task = PythonOperator(
    task_id='test_plotly',
    python_callable=test_plotly,
    dag=dag
)

combined_task = PythonOperator(
    task_id='test_combined',
    python_callable=test_combined,
    dag=dag
)

# Set task dependencies
pandas_task >> plotly_task >> combined_task