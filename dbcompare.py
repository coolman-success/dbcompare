import pymysql
from pymysql.connections import Connection
from pymysql.cursors import Cursor
from constants import DB1_CREDS, DB2_CREDS

def compare_databases(db1_conn: Connection, db2_conn: Connection):
    # Fetch schema information for database 1
    db1_schema = fetch_schema(db1_conn)

    # Fetch schema information for database 2
    db2_schema = fetch_schema(db2_conn)

    # Compare schemas and generate a comparison report
    same, diffs, db1_only, db2_only = compare_shema(db1_schema, db2_schema)

    # Print the comparison report
    comparison_report = generate_report(db1_conn, db2_conn, same, diffs, db1_only, db2_only)
    print(comparison_report)

    # Close the database connections
    db1_conn.close()
    db2_conn.close()

def fetch_schema(db_conn: Connection):
    db_name = db_conn.db.decode('ascii')
    print(f'Getting table schemas of database `{db_name}` from {db_conn.host}...')

    cursor = db_conn.cursor()

    # Fetch table names
    cursor.execute("SHOW TABLES")
    table_names = [row[0] for row in cursor]

    schema = {}

    for table_name in table_names:
        # Fetch column information for each table
        try:
            cursor.execute(f"DESCRIBE `{table_name}`")
            schema[table_name] = get_table_schema(cursor)
        except Exception as e:
            print('Error occurred while fetching table information')
            print(e)

    cursor.close()
    print(f'Getting information of `{db_name}` completed')

    return schema

def get_table_schema(cursor: Cursor):
    table_schema = set()
    # table_schema = []
    # keys = [keys[0] for keys in cursor.description]

    for column in cursor:
        # column_info = dict(zip(keys, column))
        # table_schema.append(column_info)
        table_schema.add(column)

    return table_schema

# Compare the schema details
def compare_shema(db1_schema, db2_schema):

    db1_only, db2_only, diffs, same = [], [], [], []

    for table_name, columns in db1_schema.items():
        if table_name in db2_schema:
            db2_columns = db2_schema[table_name]

            if columns == db2_columns:
                same.append(table_name)
            else:
                diffs.append((table_name, columns, db2_columns))
        else:
            db1_only.append(table_name)

    for table_name in db2_schema:
        if table_name not in db1_schema:
            db2_only.append(table_name)

    return same, diffs, db1_only, db2_only

# Generate the comparison report based on the schema details
def generate_report(db1_conn, db2_conn, same, diffs, db1_only, db2_only):
    report = ""
    db1_name = db1_conn.db.decode('ascii')
    db2_name = db2_conn.db.decode('ascii')

    if len(diffs):
        report += f"\n{len(diffs)} tables are different:\n"
        report += generate_compare_report(db1_conn, db2_conn, diffs)

    if len(db1_only):
        report += f"\n{len(db1_only)} tables exist in `{db1_name}` from {db1_conn.host} only:\n  "
        report += "\n  ".join(db1_only)

    if len(db2_only):
        report += f"\n{len(db2_only)} tables exist in `{db2_name}` from {db2_conn.host} only:\n  "
        report += "\n  ".join(db2_only)

    if len(same):
        report += f"\n{len(same)} tables are equal:\n  "
        report += "\n  ".join(same)

    return report

def generate_compare_report(db1_conn, db2_conn, diffs):
    report = ""
    db1_title = db1_conn.db.decode('ascii') + ' of ' + db1_conn.host
    db2_title = db2_conn.db.decode('ascii') + ' of ' + db2_conn.host
    
    for diff in diffs:
        table_name, x, y = diff
        x_only = x - y
        y_only = y - x
        x_only_keys = [x[0] for x in x_only]
        y_only_keys = [y[0] for y in y_only]

        deepdiff, x_new, y_new = [], [], []
        for key in x_only_keys:
            if key in y_only_keys:
                temp = [i for i in x_only if i[0] == key]
                temp += [i for i in y_only if i[0] == key]
                deepdiff.append(temp)
            else:
                x_new.append([i for i in x_only if i[0] == key])
        for key in y_only_keys:
            if key not in x_only_keys:
                y_new.append([i for i in y_only if i[0] == key])
        
        report += f"\n  {table_name}\n"
        report += f"  {db1_title[:50]:^50} | {db2_title[:50]:^50}"
        if len(deepdiff):
            report += "".join(["\n  " + f'{str(item[0])[:50]:50} | {str(item[1])[:50]:50}' for item in deepdiff])
        if len(x_new):
            report += "".join(["\n  " + f'{str(item[0])[:50]:50} |' for item in x_new])
        if len(y_new):
            report += "".join(["\n  " + f'{" "*50} | {str(item[0])[:50]:50}' for item in y_new])

    return report

# Connect to the databases
db1_conn = pymysql.connect(
    host=DB1_CREDS['host'],
    user=DB1_CREDS['user'],
    password=DB1_CREDS['password'],
    database=DB1_CREDS['database']
)

db2_conn = pymysql.connect(
    host=DB2_CREDS['host'],
    user=DB2_CREDS['user'],
    password=DB2_CREDS['password'],
    database=DB2_CREDS['database']
)

# Compare the databases
compare_databases(db1_conn, db2_conn)
