import pandas as pd
import time

#import the test tables
WS_df = pd.read_csv(r'C:\Users\surajvmudhole\Downloads\TestTable_Merge_work\TestTable_Merge_work\WS\WT1_30C_V12.csv')
RT_df = pd.read_csv(r'C:\Users\surajvmudhole\Downloads\TestTable_Merge_work\TestTable_Merge_work\FT4A\TT_30C_V15.csv')
LT_df = pd.read_csv(r'C:\Users\surajvmudhole\Downloads\TestTable_Merge_work\TestTable_Merge_work\FT1A\TT_m40C_V15.csv')
HT_df = pd.read_csv(r'C:\Users\surajvmudhole\Downloads\TestTable_Merge_work\TestTable_Merge_work\FT2A\TT_125C_V15.csv')
Merged_df = pd.read_csv(r'C:\Users\surajvmudhole\Downloads\TestTable_Merge_work\TestTable_Merge_work\Merged\All_flow_TT.csv')
print("Reading CSV's....")
time.sleep(2)

################################################### WS inster to M_TT #################################################################################
print("Read complete....\nWriting WS into M_TT...")
#ws insertion into Mdf
ws_column_mapping = {
    'Suite name': 'Suite name',
    'Test name': 'Test name',
    'Test number': 'WS Test number',
    'Lsl': 'WS Lsl',
    'Lsl_typ': 'WS Lsl_typ',
    'Usl_typ': 'WS Usl_typ',
    'Usl': 'WS Usl',
    'Units': 'WS Units'
}
# Create a new DataFrame with Merged_df's columns, filled with NaN
ws_df_mapped = pd.DataFrame(columns=Merged_df.columns)
# Fill in the mapped columns from WS_df
for src_col, dest_col in ws_column_mapping.items():
    if src_col in WS_df.columns:
        ws_df_mapped[dest_col] = WS_df[src_col]
# Append to Merged_df
Merged_df = pd.concat([Merged_df, ws_df_mapped.reindex(columns=Merged_df.columns)], ignore_index=True)
time.sleep(3)
print("WS write complete....")
########################################### Insert RT TT into Merger_TT ##########################################################################
print("Writing RT_TT into M_TT....\n")
for index, row in RT_df.iterrows():
    suite_name = row['Suite name']
    test_name = row['Test name']
    test_number = row['Test number']
    lsl = row['Lsl']
    lsl_typ = row['Lsl_typ']
    usl_typ = row['Usl_typ']
    usl = row['Usl']
    units = row['Units']
    matchSuite = Merged_df[(Merged_df['Suite name'] == suite_name) & (Merged_df['Test name'] == test_name)].index
    if not matchSuite.empty:
        Merged_df.loc[matchSuite, "FT4A_RT Test number"] = test_number
        Merged_df.loc[matchSuite, "FT4A_RT Lsl"] = lsl
        Merged_df.loc[matchSuite, "FT4A_RT Lsl_typ"] = lsl_typ
        Merged_df.loc[matchSuite, "FT4A_RT Usl_typ"] = usl_typ
        Merged_df.loc[matchSuite, "FT4A_RT Usl"] = usl
        Merged_df.loc[matchSuite, "FT4A_RT Units"] = units
    else:
        new_row={
            'Suite name':suite_name,
            'Test name': test_name,
            'FT4A_RT Test number': test_number,
            'FT4A_RT Lsl': lsl,
            'FT4A_RT Lsl_typ': lsl_typ,
            'FT4A_RT Usl_typ': usl_typ,
            'FT4A_RT Usl': usl,
            'FT4A_RT Units': units
        }
        for col in Merged_df.columns:
            if col not in new_row:
                new_row[col]=pd.NA
        new_row_df = pd.DataFrame([new_row])
        # Drop all-NA columns from new_row_df before concatenation
        new_row_df_cleaned = new_row_df.dropna(axis=1, how='all')
        # Only concatenate if there's at least one non-NA value
        if not new_row_df_cleaned.empty:
            Merged_df = pd.concat([Merged_df, new_row_df_cleaned.reindex(columns=Merged_df.columns)], ignore_index=True)
time.sleep(3)
print("Writing RT completed...")

########################################### Intert LT TT into Merger_TT ##########################################################################
print("Writing RT_TT into M_TT....\n")
for index, row in LT_df.iterrows():
    suite_name = row['Suite name']
    test_name = row['Test name']
    test_number = row['Test number']
    lsl = row['Lsl']
    lsl_typ = row['Lsl_typ']
    usl_typ = row['Usl_typ']
    usl = row['Usl']
    units = row['Units']
    matchSuite = Merged_df[(Merged_df['Suite name'] == suite_name) & (Merged_df['Test name'] == test_name)].index
    if not matchSuite.empty:
        Merged_df.loc[matchSuite, "FT1A_LT Test number"] = test_number
        Merged_df.loc[matchSuite, "FT1A_LT Lsl"] = lsl
        Merged_df.loc[matchSuite, "FT1A_LT Lsl_typ"] = lsl_typ
        Merged_df.loc[matchSuite, "FT1A_LT Usl_typ"] = usl_typ
        Merged_df.loc[matchSuite, "FT1A_LT Usl"] = usl
        Merged_df.loc[matchSuite, "FT1A_LT Units"] = units
    else:
        new_row={
            'Suite name':suite_name,
            'Test name': test_name,
            'FT1A_LT Test number': test_number,
            'FT1A_LT Lsl': lsl,
            'FT1A_LT Lsl_typ': lsl_typ,
            'FT1A_LT Usl_typ': usl_typ,
            'FT1A_LT Usl': usl,
            'FT1A_LT Units': units
        }
        for col in Merged_df.columns:
            if col not in new_row:
                new_row[col]=pd.NA
        new_row_df = pd.DataFrame([new_row])
        # Drop all-NA columns from new_row_df before concatenation
        new_row_df_cleaned = new_row_df.dropna(axis=1, how='all')
        # Only concatenate if there's at least one non-NA value
        if not new_row_df_cleaned.empty:
            Merged_df = pd.concat([Merged_df, new_row_df_cleaned.reindex(columns=Merged_df.columns)], ignore_index=True)
time.sleep(3)
print("Writing RT completed...")
########################################### Intert HT TT into Merger_TT ##########################################################################
print("Writing HT_TT into M_TT....\n")
for index, row in HT_df.iterrows():
    suite_name = row['Suite name']
    test_name = row['Test name']
    test_number = row['Test number']
    lsl = row['Lsl']
    lsl_typ = row['Lsl_typ']
    usl_typ = row['Usl_typ']
    usl = row['Usl']
    units = row['Units']
    matchSuite = Merged_df[(Merged_df['Suite name'] == suite_name) & (Merged_df['Test name'] == test_name)].index
    if not matchSuite.empty:
        Merged_df.loc[matchSuite, "FT2A_HT Test number"] = test_number
        Merged_df.loc[matchSuite, "FT2A_HT Lsl"] = lsl
        Merged_df.loc[matchSuite, "FT2A_HT Lsl_typ"] = lsl_typ
        Merged_df.loc[matchSuite, "FT2A_HT Usl_typ"] = usl_typ
        Merged_df.loc[matchSuite, "FT2A_HT Usl"] = usl
        Merged_df.loc[matchSuite, "FT2A_HT Units"] = units
    else:
        new_row={
            'Suite name':suite_name,
            'Test name': test_name,
            'FT2A_HT Test number': test_number,
            'FT2A_HT Lsl': lsl,
            'FT2A_HT Lsl_typ': lsl_typ,
            'FT2A_HT Usl_typ': usl_typ,
            'FT2A_HT Usl': usl,
            'FT2A_HT Units': units
        }
        for col in Merged_df.columns:
            if col not in new_row:
                new_row[col]=pd.NA
        new_row_df = pd.DataFrame([new_row])
        # Drop all-NA columns from new_row_df before concatenation
        new_row_df_cleaned = new_row_df.dropna(axis=1, how='all')
        # Only concatenate if there's at least one non-NA value
        if not new_row_df_cleaned.empty:
            Merged_df = pd.concat([Merged_df, new_row_df_cleaned.reindex(columns=Merged_df.columns)], ignore_index=True)

time.sleep(3)
print("Writing RT completed...")
###################################################### Generate CSV file ##############################################

print("Generating CSV...")
time.sleep(3)
Merged_df.to_csv(r'C:\Users\surajvmudhole\Downloads\TestTable_Merge_work\TestTable_Merge_work\Merged\All_flow_TT_updated.csv', index=False)
print("Generated Merged CSV...")