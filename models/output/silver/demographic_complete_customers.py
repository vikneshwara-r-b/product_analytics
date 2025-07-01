def create_output_file(dbutils, df, output_path, data_output_file_ext, data_output_container, file_name, mount_location):
    save_location = 'dbfs:/{}/{}'.format(mount_location, data_output_container)
    output_location = '{}/{}/temp.folder'.format(save_location, file_name)
    file_location = '{}/{}/{}.{}'.format(save_location, output_path, file_name,
                                            data_output_file_ext)
    df.coalesce(1).write.format(data_output_file_ext).mode("overwrite").options(header='true', delimiter='|', emptyValue="").save(output_location)
    file = dbutils.fs.ls(output_location)[-1].path
    dbutils.fs.cp(file, file_location)
    output_location_to_be_remove = '{}/{}'.format(save_location, file_name)
    dbutils.fs.rm(output_location_to_be_remove, recurse=True)

def model(dbt, session):
    # setting configuration
    df = dbt.ref("customers_latest")
    output_path = dbt.config.get("output_path")
    data_output_file_ext = dbt.config.get("data_output_file_ext")
    data_output_container = dbt.config.get("data_output_container")
    mount_location = dbt.config.get("mount_location")
    file_name = dbt.config.get("file_name")
    df = df.filter((df['is_email_valid'] == True) & (df['is_phone_valid'] == True) & (df['is_address_complete'] == True))
    metadata_cols = ["is_email_valid", "is_phone_valid","is_address_complete","first_seen_at","last_updated_at","is_deleted","_source_system","_batch_id"]
    df = df.drop(*metadata_cols)
    create_output_file(dbutils,df, output_path, data_output_file_ext, data_output_container, file_name,
                       mount_location)
    return df