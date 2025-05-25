import streamlit as st
import pandas as pd
from io import BytesIO
from openpyxl import load_workbook
from openpyxl.styles import numbers

st.set_page_config(page_title="Excel Combiner", layout="centered")
st.title("📊 Excel File Combiner")

uploaded_files = st.file_uploader(
    "Upload one or more Excel files (.xlsx)",
    type="xlsx",
    accept_multiple_files=True,
    help="Files will be concatenated vertically based on column names."
)

def convert_to_excel_with_formatting(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, na_rep='N/A')
        worksheet = writer.sheets["Sheet1"]

        # Columns to format
        id_columns = ['ID', 'User ID', 'Campaign ID', 'Ad ID', 'Phone', 'Account Number']
        large_number_columns = [
            col for col in df.columns
            if df[col].dtype in ['int64', 'float64']
            and pd.notna(df[col].max())
            and abs(df[col].max()) >= 1e10
        ]
        columns_to_format = list(set(id_columns + large_number_columns))

        for col_idx, col_name in enumerate(df.columns, start=1):
            if col_name in columns_to_format:
                for row_idx in range(2, len(df) + 2):  # row 1 is header
                    cell = worksheet.cell(row=row_idx, column=col_idx)
                    cell.number_format = '@'  # format as text
                    if isinstance(cell.value, (int, float)) and pd.notna(cell.value):
                        cell.value = str(int(cell.value))

    return output.getvalue()

if uploaded_files:
    try:
        dfs = [pd.read_excel(f) for f in uploaded_files]
        combined_df = pd.concat(dfs, ignore_index=True)

        st.success(f"✅ Successfully combined {len(uploaded_files)} files.")
        st.write(f"Total rows: {len(combined_df)}")
        st.dataframe(combined_df.head())

        excel_data = convert_to_excel_with_formatting(combined_df)
        st.download_button(
            label="📥 Download Combined Excel File",
            data=excel_data,
            file_name="combined_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"❌ Failed to combine files: {e}")
