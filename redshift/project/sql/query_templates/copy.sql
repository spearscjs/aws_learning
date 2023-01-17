/* 
    copy s3 bucket into redshift table

    :param  string -- query: string query to unload
    :param: string -- s3 bucket_name (destination)
    :param: string -- redshift_iam role
    :param: string -- delimiter: delimiter to use in CSV file
*/
COPY %s -- table name
FROM %s -- bucket path
credentials %s -- CREDENTIAL GOES HERE
CSV
DELIMITER %s -- delimiter in csv file
IGNOREHEADER 1;