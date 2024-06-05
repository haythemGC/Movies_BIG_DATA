
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when
import subprocess
import os

def show_info(df: DataFrame, name: str) -> None:
   
    print("_" + name + "_")
    df.printSchema()
    df.show()

def print_empty_values(df: DataFrame) -> None:
 
    num_rows = df.count()
    print(f"Number of rows: {num_rows}\n\nNumber of Empty Values in Each Column:")

    for i in df.columns:
        empty_count = df.filter(col(i).isNull() | isnan(col(i))).count()
        N_count = df.filter(col(i) == '\\N').count()
        print(f"{i}: {empty_count} || \\N: {N_count}")
        

        

def change_to_None(df: DataFrame) -> DataFrame:
   
    for i in df.columns:
        df = df.withColumn(i, when( isnan(col(i)) | (col(i) == '\\N'), None).otherwise(col(i)))
    return df

def print_None_values(df: DataFrame) -> None:
    
    print("Number of Empty Values in Each Column:")
    for i in df.columns:
        empty_count = df.filter(col(i).isNull()).count()
        N_count = df.filter(col(i) == '\\N').count()
        print(f"{i}: {empty_count} || \\N: {N_count}")

def save_df_to_hdfs_with_permissions(df: DataFrame, hdfs_directory: str, final_output_filename: str, local_temp_dir: str = "/tmp") -> None:
    """
    Save a DataFrame to HDFS with specific permissions, merging part files into a single CSV.

    :param df: DataFrame to be saved.
    :param hdfs_directory: HDFS directory where the file will be saved.
    :param final_output_filename: Final CSV filename in HDFS.
    :param local_temp_dir: Local directory for temporary storage.
    """
    # Create the HDFS directory if it does not exist
    create_dir_command = f"hadoop fs -mkdir -p {hdfs_directory}"
    subprocess.run(create_dir_command, shell=True, check=True)

    # Write the DataFrame to a temporary directory in HDFS
    temp_dir = f"{hdfs_directory}/temp_{final_output_filename}"
    df.write.csv(temp_dir, header=True)

    # Merge part files locally
    local_temp_file = os.path.join(local_temp_dir, final_output_filename)
    merge_command = f"hadoop fs -cat {temp_dir}/* > {local_temp_file}"
    subprocess.run(merge_command, shell=True, check=True)

    # Copy the merged file back to HDFS
    final_output = f"{hdfs_directory}/{final_output_filename}"
    copy_to_hdfs_command = f"hadoop fs -copyFromLocal {local_temp_file} {final_output}"
    subprocess.run(copy_to_hdfs_command, shell=True, check=True)

    # Set permissions on the final output file
    #chmod_final_output_command = f"hadoop fs -chmod 770 {final_output}"
    #subprocess.run(chmod_final_output_command, shell=True, check=True)

    # Remove the temporary directory from HDFS
    remove_temp_dir_command = f"hadoop fs -rm -r {temp_dir}"
    subprocess.run(remove_temp_dir_command, shell=True, check=True)

    # Clean up the local temporary file
    os.remove(local_temp_file)
