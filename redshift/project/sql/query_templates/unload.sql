/* 
    unload sql function

    :param  string -- query: strubg query to unload
    :param: string -- s3 bucket_name (destination)
    :param: string -- redshift_iam role
    :param: string -- delimiter: delimiter to use in CSV file
*/
UNLOAD (%s) -- query string
    TO %s   -- s3 bucket_name
    IAM_ROLE %s -- redshift iam role
    PARALLEL OFF
    ALLOWOVERWRITE 
    FORMAT CSV
    DELIMITER %s;   -- delimiter 