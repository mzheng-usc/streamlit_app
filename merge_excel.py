import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta


def merge_excel_data(table1_path, combined_table_path, output_path=None, target_columns=None, 
                     perform_sanity_check=True, first_date=None, second_date=None):
    """
    Merge Excel data to get statistics for a full day (midnight to midnight).
    This version:
    1. Subtracts Table 1 from Combined Table (first_date) to get midnight-to-3pm data
    2. Combines rows with the same group ID that appear in both midnight-to-3pm and second_date data
    3. Keeps rows that only appear in one dataset
    
    Parameters:
    - table1_path: Path to Excel file with data from 3pm to midnight on first_date
    - combined_table_path: Path to Excel file with combined data
    - output_path: Optional path to save the resulting DataFrame
    - target_columns: Optional list of column indices to save in the output file
    - perform_sanity_check: Whether to perform and print a sanity check
    - first_date: First date to process (e.g., '2025-05-20'). If None, defaults to '2025-05-20'
    - second_date: Second date to process (e.g., '2025-05-21'). If None, defaults to day after first_date
    
    Returns:
    - DataFrame with combined data for a full day (midnight second_date to midnight next day)
    """
    # Set default dates if not provided
    if first_date is None:
        first_date = '2025-05-20'
    if second_date is None:
        # If second_date not provided, use the day after first_date
        first_dt = pd.to_datetime(first_date)
        second_dt = first_dt + timedelta(days=1)
        second_date = second_dt.strftime('%Y-%m-%d')
    
    # Convert to datetime objects
    first_dt = pd.to_datetime(first_date)
    second_dt = pd.to_datetime(second_date)
    
    # Create formatted date strings for display
    first_date_display = first_dt.strftime('%Y-%m-%d(%a)')
    second_date_display = second_dt.strftime('%Y-%m-%d(%a)')
    
    print("读取Excel文件...")
    print(f"处理日期: {first_date} (第一天) 和 {second_date} (第二天)")
    
    # Read the Excel files
    table1 = pd.read_excel(table1_path)
    combined_table = pd.read_excel(combined_table_path)
    
    print(f"表1 形状: {table1.shape}")
    print(f"组合表 形状: {combined_table.shape}")
    
    # Define grouping columns for creating the composite group ID
    grouping_columns = ['书籍ID', '渠道ID', 'Ad Group ID', 'Ad ID']
    
    # Create a function to generate composite group ID
    def create_group_id(df):
        # Convert all columns to string and join them
        # First clean up any potential trailing tabs or spaces in the ID columns
        df_copy = df.copy()
        for col in grouping_columns:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].astype(str).str.strip()
        
        return df_copy[grouping_columns].astype(str).agg('_'.join, axis=1)
    
    # Add group_id column to both DataFrames
    table1['group_id'] = create_group_id(table1)
    combined_table['group_id'] = create_group_id(combined_table)
    
    # Create a clean date extractor function
    def extract_date(date_str):
        if isinstance(date_str, str):
            # Extract just the date part before any parenthesis
            match = re.match(r'(\d{4}-\d{2}-\d{2})', date_str)
            if match:
                return match.group(1)
        return date_str
    
    # Extract clean dates for filtering
    table1['clean_date'] = table1['日期'].apply(extract_date)
    combined_table['clean_date'] = combined_table['日期'].apply(extract_date)
    
    # Convert to datetime for comparison
    table1['clean_date'] = pd.to_datetime(table1['clean_date'])
    combined_table['clean_date'] = pd.to_datetime(combined_table['clean_date'])
    
    # Filter table1 for first_date data (3pm to midnight)
    table1_first = table1[table1['clean_date'] == first_dt].copy()
    
    # Filter combined table
    combined_first = combined_table[combined_table['clean_date'] == first_dt].copy()  # 3pm to 3pm next day
    combined_second = combined_table[combined_table['clean_date'] == second_dt].copy()  # 3pm to midnight next day
    
    print(f"\n筛选后的数据形状:")
    print(f"表1 ({first_date}): {table1_first.shape}")
    print(f"组合表 ({first_date}): {combined_first.shape}")
    print(f"组合表 ({second_date}): {combined_second.shape}")
    
    # Check for duplicate group IDs within each date group
    print("\n验证每个日期内的组ID唯一性:")
    
    for df, name in [(table1_first, f"表1 ({first_date})"), 
                     (combined_first, f"组合表 ({first_date})"), 
                     (combined_second, f"组合表 ({second_date})")]:
        group_id_counts = df['group_id'].value_counts()
        duplicates = group_id_counts[group_id_counts > 1]
        
        if not duplicates.empty:
            print(f"{name} 中存在重复的组ID:")
            for group_id, count in duplicates.items():
                print(f"  组ID: {group_id} - 出现 {count} 次")
        else:
            print(f"{name} 中的组ID都是唯一的")
    
    # Identify all numeric columns for calculations
    non_numeric_cols = ['日期', 'clean_date', 'group_id', '书籍ID', '书籍名称(书籍ID)', 
                       '对应英语书籍名称', '书籍变现类型', '媒体类型', '渠道名称', '渠道ID',
                       'Ad Group Name', 'Ad Group ID', 'Ad Name', 'Ad ID']
    
    # Get numeric columns
    numeric_cols = [col for col in combined_table.columns 
                   if col not in non_numeric_cols and pd.api.types.is_numeric_dtype(combined_table[col])]
    
    print(f"\n识别到的数值列数量: {len(numeric_cols)}")
    
    # STEP 1: Calculate midnight to 3pm data (Combined first_date - Table 1)
    # Create dictionaries to store the values by group_id
    table1_values = {}
    combined_first_values = {}
    
    # Extract values from table1
    for _, row in table1_first.iterrows():
        group_id = row['group_id']
        table1_values[group_id] = {col: row[col] for col in numeric_cols}
    
    # Extract values from combined_first
    for _, row in combined_first.iterrows():
        group_id = row['group_id']
        combined_first_values[group_id] = {col: row[col] for col in numeric_cols}
    
    # Create the midnight to 3pm dataset
    midnight_to_3pm_rows = []
    
    # For each group in combined_first, subtract table1 values if available
    for group_id, combined_values in combined_first_values.items():
        # Create a new row with the group_id and metadata
        new_row = next((row.copy() for _, row in combined_first.iterrows() if row['group_id'] == group_id), None)
        
        if new_row is not None:
            # Set the date to second_date since this is midnight to 3pm on second_date
            new_row['日期'] = second_date_display
            new_row['clean_date'] = second_dt
            
            # Subtract table1 values for numeric columns
            if group_id in table1_values:
                for col in numeric_cols:
                    new_row[col] = combined_values.get(col, 0) - table1_values.get(group_id, {}).get(col, 0)
                    
                    # For count-like columns, floor at 0
                    if not any(substr in col.lower() for substr in ['rate', 'ratio', 'roas', '率', '比例']):
                        new_row[col] = max(0, new_row[col])
            
            midnight_to_3pm_rows.append(new_row)
    
    # Convert to DataFrame
    midnight_to_3pm_df = pd.DataFrame(midnight_to_3pm_rows)
    
    print(f"\n午夜到下午3点数据形状: {midnight_to_3pm_df.shape}")
    
    # STEP 2: Identify groups that appear in both midnight-to-3pm and second_date data
    if not midnight_to_3pm_df.empty and not combined_second.empty:
        midnight_group_ids = set(midnight_to_3pm_df['group_id'])
        second_group_ids = set(combined_second['group_id'])
        
        # Find overlapping group IDs
        common_group_ids = midnight_group_ids.intersection(second_group_ids)
        only_in_midnight = midnight_group_ids - second_group_ids
        only_in_second = second_group_ids - midnight_group_ids
        
        print(f"\n组ID分析:")
        print(f"午夜到3点数据中唯一组ID数: {len(midnight_group_ids)}")
        print(f"{second_date}数据中唯一组ID数: {len(second_group_ids)}")
        print(f"两个数据集中都出现的组ID数: {len(common_group_ids)}")
        print(f"仅在午夜到3点数据中出现的组ID数: {len(only_in_midnight)}")
        print(f"仅在{second_date}数据中出现的组ID数: {len(only_in_second)}")
        
        # STEP 3: Create a new dataframe that combines rows with the same group ID
        # Start with rows that only appear in one dataset
        rows_to_keep = []
        
        # Add rows that only appear in midnight-to-3pm
        midnight_only_rows = midnight_to_3pm_df[midnight_to_3pm_df['group_id'].isin(only_in_midnight)]
        rows_to_keep.append(midnight_only_rows)
        
        # Add rows that only appear in second_date
        second_only_rows = combined_second[combined_second['group_id'].isin(only_in_second)]
        rows_to_keep.append(second_only_rows)
        
        # For common group IDs, combine the rows by adding numeric values
        combined_rows = []
        for group_id in common_group_ids:
            # Get the rows from each dataset
            midnight_row = midnight_to_3pm_df[midnight_to_3pm_df['group_id'] == group_id].iloc[0]
            second_row = combined_second[combined_second['group_id'] == group_id].iloc[0]
            
            # Create a new row with the combined values
            combined_row = midnight_row.copy()
            
            # Sum numeric columns
            for col in numeric_cols:
                if col in midnight_row and col in second_row:
                    combined_row[col] = midnight_row[col] + second_row[col]
            
            combined_rows.append(combined_row)
        
        # Convert combined rows to DataFrame if there are any
        if combined_rows:
            combined_rows_df = pd.DataFrame(combined_rows)
            rows_to_keep.append(combined_rows_df)
        
        # Concatenate all rows to create the final dataset
        full_day_data = pd.concat(rows_to_keep, ignore_index=True)
        
        # Report on the combination
        print(f"\n合并后数据形状: {full_day_data.shape}")
        print(f"行数: {len(full_day_data)}")
        print(f"期望行数: {len(only_in_midnight) + len(only_in_second) + len(common_group_ids)}")
        
        # If there were groups that were combined, show an example
        if common_group_ids:
            sample_group = next(iter(common_group_ids))
            midnight_row = midnight_to_3pm_df[midnight_to_3pm_df['group_id'] == sample_group].iloc[0]
            second_row = combined_second[combined_second['group_id'] == sample_group].iloc[0]
            combined_row = full_day_data[full_day_data['group_id'] == sample_group].iloc[0]
            
            print(f"\n合并示例 (组ID: {sample_group}):")
            check_cols = ['投放花费', 'd0', '应用设备激活数']
            available_cols = [col for col in check_cols if col in numeric_cols]
            
            print("各数据源值:")
            table_format = "{:<20} {:<15} {:<15} {:<15}"
            header = ["数据源", "投放花费", "d0", "应用设备激活数"]
            available_header = ["数据源"] + [col for col in header[1:] if col in available_cols]
            
            print(table_format.format(*header[:len(available_header)]))
            print("-" * 60)
            
            # Function to format a row for display
            def format_row(name, row):
                values = [name]
                for col in available_cols:
                    values.append(str(row.get(col, "N/A")))
                return table_format.format(*values)
            
            print(format_row("午夜到3点", midnight_row))
            print(format_row(f"{second_date} (3点到午夜)", second_row))
            print(format_row("合并后", combined_row))
            
            # Verify the math is correct
            print("\n验证合并计算:")
            for col in available_cols:
                expected = midnight_row[col] + second_row[col]
                actual = combined_row[col]
                print(f"{col}: {midnight_row[col]} + {second_row[col]} = {expected}, 实际值: {actual}, " +
                      f"{'✓ 正确' if abs(expected - actual) < 0.001 else '✗ 错误'}")
    else:
        # If either dataset is empty, just use what we have
        if midnight_to_3pm_df.empty:
            full_day_data = combined_second.copy()
            print(f"\n午夜到3点数据为空, 仅使用{second_date}数据")
        elif combined_second.empty:
            full_day_data = midnight_to_3pm_df.copy()
            print(f"\n{second_date}数据为空, 仅使用午夜到3点数据")
        else:
            full_day_data = pd.DataFrame()  # Empty dataframe if both are empty
            print("\n警告: 两个数据集都为空!")
    
    # Print summary statistics
    print("\n完整天数据统计:")
    if '投放花费' in full_day_data.columns:
        print(f"总投放花费: {full_day_data['投放花费'].sum():.2f}")
    
    if 'revenue(生命周期)' in full_day_data.columns and '投放花费' in full_day_data.columns:
        total_revenue = full_day_data['revenue(生命周期)'].sum()
        total_cost = full_day_data['投放花费'].sum()
        if total_cost > 0:
            roi = total_revenue / total_cost
            print(f"总收入: {total_revenue:.2f}")
            print(f"ROI: {roi:.2f}")
    
    # Count unique group IDs
    unique_groups = full_day_data['group_id'].nunique()
    print(f"\n唯一组ID数: {unique_groups}")
    print(f"总行数: {len(full_day_data)}")
    
    # Perform detailed sanity check if requested
    if perform_sanity_check:
        print("\n============== 详细数据完整性检查 ==============")
        
        # Find a group ID that exists in all three datasets (ideal case)
        common_to_all = set(table1_first['group_id']).intersection(
            set(combined_first['group_id']), set(combined_second['group_id']))
        
        if common_to_all:
            sample_id = next(iter(common_to_all))
            print(f"找到存在于所有数据集的组ID: {sample_id}")
        else:
            # Find a group ID that exists in table1 and combined_first at minimum
            common_to_t1_cm = set(table1_first['group_id']).intersection(set(combined_first['group_id']))
            if common_to_t1_cm:
                sample_id = next(iter(common_to_t1_cm))
                print(f"找到存在于表1和组合表({first_date})的组ID: {sample_id}")
                
                # Check if this ID also exists in the final data
                if sample_id in set(full_day_data['group_id']):
                    print("该组ID也存在于最终数据中")
                else:
                    print("警告: 该组ID不存在于最终数据中!")
            else:
                print("无法找到用于检查的共同组ID")
                sample_id = None
        
        # If we found a sample ID, show all its values and calculations
        if sample_id:
            # Extract individual components of the group ID
            ids = sample_id.split('_')
            if len(ids) >= 4:
                print(f"\n组ID组成:")
                print(f"书籍ID: {ids[0]}")
                print(f"渠道ID: {ids[1]}")
                print(f"Ad Group ID: {ids[2]}")
                print(f"Ad ID: {ids[3]}")
            
            # Get values from each dataset
            check_cols = ['投放花费', 'd0', '应用设备激活数']
            available_cols = [col for col in check_cols if col in numeric_cols]
            
            # Function to get a row from a DataFrame by group_id
            def get_row_values(df, gid, cols):
                row = df[df['group_id'] == gid]
                if len(row) > 0:
                    return {col: row[col].iloc[0] for col in cols if col in row.columns}
                return {col: "N/A" for col in cols}
            
            # Get values from each source table
            table1_vals = get_row_values(table1_first, sample_id, available_cols)
            cm_first_vals = get_row_values(combined_first, sample_id, available_cols)
            cm_second_vals = get_row_values(combined_second, sample_id, available_cols)
            final_vals = get_row_values(full_day_data, sample_id, available_cols)
            
            # Print a table with all values
            print("\n原始数据和计算步骤:")
            table_format = "{:<20} {:<15} {:<15} {:<15}"
            header = ["数据源", "投放花费", "d0", "应用设备激活数"]
            
            print(table_format.format(*header[:len(available_cols)+1]))
            print("-" * 65)
            
            # Print original values
            print(table_format.format(f"表1 ({first_date})", 
                                     str(table1_vals.get('投放花费', "N/A")),
                                     str(table1_vals.get('d0', "N/A")),
                                     str(table1_vals.get('应用设备激活数', "N/A"))))
            
            print(table_format.format(f"组合表 ({first_date})", 
                                     str(cm_first_vals.get('投放花费', "N/A")),
                                     str(cm_first_vals.get('d0', "N/A")),
                                     str(cm_first_vals.get('应用设备激活数', "N/A"))))
            
            # Calculate and print the midnight-to-3pm values
            midnight_vals = {}
            for col in available_cols:
                if col in table1_vals and col in cm_first_vals:
                    val = cm_first_vals[col] - table1_vals[col]
                    if not any(substr in col.lower() for substr in ['rate', 'ratio', 'roas', '率', '比例']):
                        val = max(0, val)
                    midnight_vals[col] = val
            
            print(f"\n步骤1: 计算午夜到下午3点的数据")
            print(table_format.format(f"步骤1计算: {first_date}组合-表1", 
                                     f"{cm_first_vals.get('投放花费', 'N/A')} - {table1_vals.get('投放花费', 'N/A')} = {midnight_vals.get('投放花费', 'N/A')}",
                                     f"{cm_first_vals.get('d0', 'N/A')} - {table1_vals.get('d0', 'N/A')} = {midnight_vals.get('d0', 'N/A')}",
                                     f"{cm_first_vals.get('应用设备激活数', 'N/A')} - {table1_vals.get('应用设备激活数', 'N/A')} = {midnight_vals.get('应用设备激活数', 'N/A')}"))
            
            # If also present in second_date data, show the addition
            if sample_id in set(combined_second['group_id']):
                print(f"\n组合表 ({second_date}) 中的值:")
                print(table_format.format(f"组合表 ({second_date})", 
                                         str(cm_second_vals.get('投放花费', "N/A")),
                                         str(cm_second_vals.get('d0', "N/A")),
                                         str(cm_second_vals.get('应用设备激活数', "N/A"))))
                
                # Calculate combined values
                combined_vals = {}
                for col in available_cols:
                    if col in midnight_vals and col in cm_second_vals:
                        combined_vals[col] = midnight_vals[col] + cm_second_vals[col]
                
                print(f"\n步骤2: 合并午夜到下午3点和下午3点到午夜的数据")
                print(table_format.format(f"步骤2计算: 午夜到3点+{second_date}", 
                                         f"{midnight_vals.get('投放花费', 'N/A')} + {cm_second_vals.get('投放花费', 'N/A')} = {combined_vals.get('投放花费', 'N/A')}",
                                         f"{midnight_vals.get('d0', 'N/A')} + {cm_second_vals.get('d0', 'N/A')} = {combined_vals.get('d0', 'N/A')}",
                                         f"{midnight_vals.get('应用设备激活数', 'N/A')} + {cm_second_vals.get('应用设备激活数', 'N/A')} = {combined_vals.get('应用设备激活数', 'N/A')}"))
            else:
                print(f"\n该组ID不存在于{second_date}数据中, 因此最终值应与午夜到下午3点的值相同")
            
            # Show the final values
            print("\n最终数据中的值:")
            print(table_format.format("最终数据", 
                                     str(final_vals.get('投放花费', "N/A")),
                                     str(final_vals.get('d0', "N/A")),
                                     str(final_vals.get('应用设备激活数', "N/A"))))
            
            # Verify the calculation is correct
            expected_vals = combined_vals if 'combined_vals' in locals() else midnight_vals
            verification_ok = True
            
            print("\n计算验证:")
            for col in available_cols:
                if col in expected_vals and col in final_vals:
                    is_correct = abs(expected_vals[col] - final_vals[col]) < 0.001
                    if not is_correct:
                        verification_ok = False
                    print(f"{col}: 期望值 = {expected_vals[col]}, 实际值 = {final_vals[col]}, " +
                          f"{'✓ 正确' if is_correct else '✗ 错误'}")
            
            if verification_ok:
                print("\n✅ 所有计算验证通过, 数据处理正确!")
            else:
                print("\n❌ 验证失败! 数据处理有误!")
        
        print("==============================================")
    
    # Save the result if an output path is provided
    if output_path and not full_day_data.empty:
        print(f"\n正在保存结果到 {output_path}...")
        
        # Create a copy of the data to avoid modifying the original
        output_data = full_day_data.copy()
        
        # Clean up temporary columns
        if 'clean_date' in output_data.columns:
            output_data = output_data.drop(columns=['clean_date'])
        if 'group_id' in output_data.columns:
            output_data = output_data.drop(columns=['group_id'])
        
        # Re-order columns to match original format
        original_columns = combined_table.columns.tolist()
        output_columns = [col for col in original_columns if col in output_data.columns]
        output_data = output_data[output_columns]
        
        # Apply target columns filtering if specified
        if target_columns is not None:
            # Make sure target_columns are valid
            valid_indices = [i for i in target_columns if i < len(output_data.columns)]
            if len(valid_indices) > 0:
                # Show which columns we're saving
                target_column_names = [output_data.columns[i] for i in valid_indices]
                print(f"仅保存 {len(valid_indices)} 个目标列: {target_column_names}")
                output_data = output_data.iloc[:, valid_indices]
            else:
                print("警告: 提供的目标列索引无效, 保存所有列")
        else:
            print("未指定目标列, 保存所有列")
        
        # Set the final output date (use the day after second_date for the result)
        next_day = second_dt + timedelta(days=1)
        output_date_display = next_day.strftime('%Y-%m-%d(%a)')
        output_data['日期'] = output_date_display
        
        # Save to Excel with proper formatting to avoid scientific notation
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            output_data.to_excel(writer, index=False)
            
            # Get the worksheet
            worksheet = writer.sheets['Sheet1']
            
            # Apply formatting to prevent scientific notation for large numbers
            # First identify columns that might contain large IDs
            id_columns = ['渠道ID', 'Ad Group ID', 'Ad ID']
            
            # Find the column indices
            for col_idx, col_name in enumerate(output_data.columns, 1):  # Excel is 1-indexed
                if col_name in id_columns:
                    # Apply text format to these columns
                    for row_idx in range(2, len(output_data) + 2):  # +2 for header row and 1-indexing
                        cell = worksheet.cell(row=row_idx, column=col_idx)
                        cell.number_format = '@'  # Text format
                        
                        # If the cell contains a number, convert it to string to preserve full value
                        if isinstance(cell.value, (int, float)):
                            cell.value = str(int(cell.value))
        
        print(f"保存完成! 文件包含 {len(output_data.columns)} 列, {len(output_data)} 行")
        print(f"输出日期设置为: {output_date_display}")
    
    # Return the requested columns
    if not full_day_data.empty:
        if target_columns is not None:
            valid_indices = [i for i in target_columns if i < len(full_day_data.columns)]
            if len(valid_indices) > 0:
                selected_columns = full_day_data.iloc[:, valid_indices]
                print(f"返回 DataFrame 包含 {len(valid_indices)} 个目标列, {len(selected_columns)} 行")
                return selected_columns
        
        print(f"返回完整 DataFrame, 包含 {len(full_day_data.columns)} 列, {len(full_day_data)} 行")
        return full_day_data
    else:
        print("返回空 DataFrame")
        return pd.DataFrame()
