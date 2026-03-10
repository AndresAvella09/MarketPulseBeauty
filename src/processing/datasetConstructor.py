import pandas as pd
from pathlib import Path


class DatasetConstructor:
    """Crea conjuntos de datos procesados a partir de una fuente de entrada.

    Parámetros
    - dataset: ruta de archivo a un CSV (str o Path) o un pandas.DataFrame.

    Nota: aquí no se admite pasar un directorio; utiliza el script de orquestación
    (`scripts/process_google_trends.py`) para iterar archivos en una carpeta.
    """
    def __init__(self, dataset):
        if isinstance(dataset, (str, Path)):
            p = Path(dataset)
            if p.exists() and p.is_dir():
                raise ValueError(
                    "`dataset` is a directory. Pass a CSV file path or a DataFrame. "
                    "To process a folder, use the orchestration script that iterates files."
                )
        self.dataset = dataset

    def construct(self):
        pass

    def separe_google_dataset(self, output_dir: str = "data/processed/google_trends"):
        df = pd.read_csv(self.dataset) if isinstance(self.dataset, (str, Path)) else self.dataset.copy()
        if "keyword" not in df.columns:
            raise ValueError("Input dataset must contain a 'keyword' column")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        for keyword, group in df.groupby("keyword"):
            keyword_df = group.drop(columns=["keyword"])
            safe_name = keyword.replace(" ", "_").replace("/", "-")
            keyword_df.to_csv(output_path / f"{safe_name}.csv", index=False)
