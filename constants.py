# Mapping of MySQL data type codes to data types
MYSQL_DATA_TYPES = {
    0: 'DECIMAL',
    1: 'TINY',
    2: 'SHORT',
    3: 'LONG',
    4: 'FLOAT',
    5: 'DOUBLE',
    6: 'NULL',
    7: 'TIMESTAMP',
    8: 'LONGLONG',
    9: 'INT24',
    10: 'DATE',
    11: 'TIME',
    12: 'DATETIME',
    13: 'YEAR',
    14: 'NEWDATE',
    15: 'VARCHAR',
    16: 'BIT',
    245: 'JSON',
    246: 'NEWDECIMAL',
    247: 'ENUM',
    248: 'SET',
    249: 'TINY_BLOB',
    250: 'MEDIUM_BLOB',
    251: 'LONG_BLOB',
    252: 'BLOB',
    253: 'VAR_STRING',
    254: 'STRING',
    255: 'GEOMETRY'
}

DB1_CREDS = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'erp'
}

DB2_CREDS = {
    'host': 'proddatabase.cluster-ccqu0zc078pv.us-west-2.rds.amazonaws.com',
    'user': 'production',
    'password': '2orxallxmata',
    'database': 'erp'
}
