import pandas as pd
import numpy as np
from pathlib import Path

class HealthScoreCalculator:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)

    def load_data(self):
        """Carga el archivo parquet con los sentimientos ya calculados."""
        return pd.read_parquet(self.input_path)

    def calculate_metrics(self, df):
        """
        Calcula el HS siguiendo la lógica de promedio ponderado.
        Se asume que el DF tiene: 'category', 'product_id', 'sentiment_score', 'mention_count'
        """
        category_stats = df.groupby('category').apply(
            lambda x: pd.Series({
                'weighted_sentiment': (x['sentiment_score'] * x['mention_count']).sum(),
                'total_volume': x['mention_count'].sum()
            })
        ).reset_index()

        category_stats['health_score'] = (
            (category_stats['weighted_sentiment'] / category_stats['total_volume']) + 1
        ) * 50
        
        return category_stats[['category', 'health_score', 'total_volume']]

    def save_results(self, result_df):
        """Guarda el resultado en parquet para que el dashboard lo consuma."""
        result_df.to_parquet(self.output_path)
        print(f"Health Scores calculados y guardados en {self.output_path}")

if __name__ == "__main__":
    # Configuración de rutas (puedes usar variables de entorno o config.yaml)
    INPUT = "data/processed/sentiment_results.parquet"
    OUTPUT = "data/processed/category_health_scores.parquet"
    
    calculator = HealthScoreCalculator(INPUT, OUTPUT)
    data = calculator.load_data()
    scores = calculator.calculate_metrics(data)
    calculator.save_results(scores)