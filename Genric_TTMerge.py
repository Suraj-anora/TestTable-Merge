import os
import pandas as pd
import glob

# Get the directory where input files are kept
project_dir = os.path.dirname(os.path.abspath(__file__))
input_dir = os.path.join(project_dir, 'Test_Tables')
input_dir = os.path.abspath(input_dir)

# Get all CSV files
input_files = glob.glob(os.path.join(input_dir, '*.csv'))

# Fixed columns
starting_columns = ['Suite name', 'Test name']
ending_columns = ['SoftBin', 'HardBin', 'Description']
master_df = pd.DataFrame(columns=starting_columns + ending_columns)

for file_path in input_files:
    file_label = os.path.splitext(os.path.basename(file_path))[0]
    df = pd.read_csv(file_path)

    if not all(col in df.columns for col in
               starting_columns + ['Test number', 'Lsl', 'Lsl_typ', 'Usl_typ', 'Usl', 'Units']):
        continue

    for _, row in df.iterrows():
        match = ((master_df['Suite name'] == row['Suite name']) &
                 (master_df['Test name'] == row['Test name']))

        if match.any():
            idx = master_df[match].index[0]
        else:
            idx = len(master_df)
            master_df.loc[idx, 'Suite name'] = row['Suite name']
            master_df.loc[idx, 'Test name'] = row['Test name']
            master_df.loc[idx, 'SoftBin'] = row.get('SoftBin', '')
            master_df.loc[idx, 'HardBin'] = row.get('HardBin', '')
            master_df.loc[idx, 'Description'] = row.get('Description', '')

        master_df.loc[idx, f'{file_label} Test number'] = row['Test number']
        master_df.loc[idx, f'{file_label} Lsl'] = row['Lsl']
        master_df.loc[idx, f'{file_label} Lsl_typ'] = row['Lsl_typ']
        master_df.loc[idx, f'{file_label} Usl_typ'] = row['Usl_typ']
        master_df.loc[idx, f'{file_label} Usl'] = row['Usl']
        master_df.loc[idx, f'{file_label} Units'] = row['Units']

# Reorder columns: fixed start, dynamic sorted, fixed end
fixed_start = starting_columns
fixed_end = ending_columns
dynamic_cols = [col for col in master_df.columns if col not in fixed_start + fixed_end]
ordered_cols = fixed_start + sorted(dynamic_cols) + fixed_end
master_df = master_df.reindex(columns=ordered_cols)

# Output path (same as input directory)
output_file = os.path.join(input_dir, 'All_flow_TT_merged.csv')
master_df.to_csv(output_file, index=False)

print(f'Merged file saved to: {output_file}')
