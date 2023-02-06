/* 
    copy s3 bucket into redshift table

    :param string -- schema: schema table is in
    :param  string -- table_name: name of table to copy into
    :param: string -- s3 bucket_name (destination)
    :param: string -- redshift_iam role
    :param: string -- delimiter: delimiter to use in CSV file
*/
COPY {}.{} -- schema, table name
FROM %s -- bucket path
credentials %s -- CREDENTIAL GOES HERE
CSV
DELIMITER %s -- delimiter in csv file
IGNOREHEADER 1;