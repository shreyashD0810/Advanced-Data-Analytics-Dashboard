# data_manager.py
import pandas as pd
import duckdb
import os
import tempfile
from typing import Dict, List, Any
import re

class DataManager:
    def __init__(self):
        self.current_dataset = None
        self.datasets = {}
        
    def connect(self):
        """No persistent DB needed – datasets live in memory"""
        pass  # DuckDB works in-memory

    def upload_dataset(self, file, dataset_name: str) -> Dict[str, Any]:
        """Upload and process a dataset file"""
        try:
            file_extension = file.name.split('.')[-1].lower()
            
            if file_extension in ['csv', 'txt']:
                df = pd.read_csv(file)
            elif file_extension in ['xlsx', 'xls']:
                df = pd.read_excel(file)
            elif file_extension == 'json':
                df = pd.read_json(file)
            else:
                return {"error": f"Unsupported file format: {file_extension}"}
            
            # Store in memory
            self.datasets[dataset_name] = {
                'dataframe': df,
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': df.columns.tolist(),
                'dtypes': df.dtypes.astype(str).to_dict()
            }
            
            self.current_dataset = dataset_name
            
            return {
                "success": True,
                "dataset_name": dataset_name,
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": df.columns.tolist(),
                "preview": df.head(10).to_dict('records')
            }
            
        except Exception as e:
            return {"error": f"Error processing file: {str(e)}"}
    
    def get_dataset_names(self) -> List[str]:
        return list(self.datasets.keys())
    
    def set_current_dataset(self, dataset_name: str) -> bool:
        if dataset_name in self.datasets:
            self.current_dataset = dataset_name
            return True
        return False
    
    def get_dataset_info(self, dataset_name: str = None) -> Dict[str, Any]:
        name = dataset_name or self.current_dataset
        if name not in self.datasets:
            return {"error": "Dataset not found"}
        
        df = self.datasets[name]['dataframe']
        return {
            "name": name,
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": df.columns.tolist(),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "preview": df.head(5).to_dict('records')
        }

    def query_data(self, query: str, dataset_name: str = None) -> List[Dict[str, Any]]:
        """Execute SQL on dataset using DuckDB with proper column quoting"""
        name = dataset_name or self.current_dataset
        if name not in self.datasets:
            return [{"error": "No dataset selected"}]
        
        df = self.datasets[name]['dataframe']
        conn = duckdb.connect()

        try:
            # Register the DataFrame
            conn.register('current_data', df)

            # Smart quote column names
            quoted_query = self._quote_column_names_smart(query, df.columns.tolist())
            
            # Execute
            result_df = conn.execute(quoted_query).fetchdf()
            conn.close()
            
            return result_df.to_dict('records')
            
        except Exception as e:
            conn.close()
            error_msg = str(e)
            return [{"error": self._enhance_error(error_msg, df.columns.tolist())}]

    def _quote_column_names_smart(self, query: str, columns: List[str]) -> str:
        """
        Replace backticks with double quotes and handle unquoted column names.
        Works with spaces, special characters, and mixed case.
        """
        # Step 1: Replace backticks with double quotes
        quoted = query.replace('`', '"')
        
        # Step 2: Find and quote unquoted column references
        # Sort columns by length (longest first) to avoid partial matches
        sorted_cols = sorted(columns, key=len, reverse=True)
        
        for col in sorted_cols:
            # Skip if column is already quoted in the query
            if f'"{col}"' in quoted:
                continue
                
            # Create pattern to match unquoted column names
            # Match whole words only, case-insensitive
            escaped_col = re.escape(col)
            
            # Pattern matches column name not inside quotes
            # Uses negative lookbehind and lookahead to avoid matching inside strings
            pattern = r'(?<!")(?<!\w)' + escaped_col + r'(?!\w)(?!")'
            
            # Replace with quoted version
            quoted = re.sub(pattern, f'"{col}"', quoted, flags=re.IGNORECASE)
        
        return quoted

    def _enhance_error(self, error: str, columns: List[str]) -> str:
        """Make DuckDB errors user-friendly"""
        error_lower = error.lower()
        
        if "column" in error_lower and "not found" in error_lower:
            # Extract potential bad column
            match = re.search(r'"([^"]+)"|`([^`]+)`|(\w+)', error)
            if match:
                bad_col = (match.group(1) or match.group(2) or match.group(3)).strip()
                similar = [c for c in columns if bad_col.lower() in c.lower()]
                if similar:
                    return f"Column '{bad_col}' not found. Did you mean: {', '.join(similar[:3])}?"
                else:
                    return f"Column '{bad_col}' not found. Available columns: {', '.join(columns[:5])}"
        
        if "syntax error" in error_lower:
            return f"SQL syntax error. Check your query syntax and column names."
        
        if "binder" in error_lower or "catalog" in error_lower:
            return f"Column reference error. Available columns: {', '.join(columns[:5])}"
        
        return f"Query failed: {error}"

    def get_analysis(self, dataset_name: str = None) -> Dict[str, Any]:
        name = dataset_name or self.current_dataset
        if name not in self.datasets:
            return {"error": "No dataset selected"}
        
        df = self.datasets[name]['dataframe']
        
        analysis = {
            "basic_stats": {},
            "column_analysis": {},
            "correlations": {}
        }
        
        analysis["basic_stats"] = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "missing_values": int(df.isnull().sum().sum()),
            "duplicate_rows": int(df.duplicated().sum())
        }
        
        for column in df.columns:
            col_data = df[column]
            stats = {
                "dtype": str(col_data.dtype),
                "unique_values": int(col_data.nunique()),
                "missing_values": int(col_data.isnull().sum()),
                "sample_values": col_data.dropna().head(5).tolist()
            }
            
            if pd.api.types.is_numeric_dtype(col_data):
                stats.update({
                    "mean": float(col_data.mean()),
                    "median": float(col_data.median()),
                    "std": float(col_data.std()),
                    "min": float(col_data.min()),
                    "max": float(col_data.max())
                })
            
            analysis["column_analysis"][column] = stats
        
        numeric_df = df.select_dtypes(include=['number'])
        if len(numeric_df.columns) > 1:
            analysis["correlations"] = numeric_df.corr().round(3).to_dict()
        
        return analysis

# Global instance
data_manager = DataManager()