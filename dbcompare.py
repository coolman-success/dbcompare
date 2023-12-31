import pymysql
from pymysql.connections import Connection
from pymysql.cursors import Cursor
from constants import DB1_CREDS, DB2_CREDS

# Compare two different databases
def compare_databases(db1_conn: Connection, db2_conn: Connection) -> None:
    # Fetch schema information for database 1
    db1_schema = fetch_schema(db1_conn)

    # Fetch schema information for database 2
    db2_schema = fetch_schema(db2_conn)

    # Compare schemas and generate a comparison report
    same, diffs, db1_only, db2_only = compare_shema(db1_schema, db2_schema)

    # Print the comparison report
    comparison_report = generate_report(db1_conn, db2_conn, same, diffs, db1_only, db2_only)
    # print(comparison_report)
    print('Writing comparison report to report.txt...')
    write_outputs_to_file('report.txt', comparison_report)

    # Generate SQLs to make the databases consistent
    sql_statements = generate_sql_statements(diffs, db1_only, db2_only)
    # print(sql_statements)
    print('Writing SQL statements to sync.sql...')
    write_outputs_to_file('sync.sql', sql_statements)

    # Close the database connections
    db1_conn.close()
    db2_conn.close()

# Fetch table schemas from a connection
def fetch_schema(db_conn: Connection) -> dict:
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

# Get table schema in set
def get_table_schema(cursor: Cursor) -> set:
    table_schema = set()
    # table_schema = []
    # keys = [keys[0] for keys in cursor.description]

    for column in cursor:
        # column_info = dict(zip(keys, column))
        # table_schema.append(column_info)
        table_schema.add(column)

    return table_schema

# Compare the schema details
def compare_shema(db1_schema: dict, db2_schema: dict) -> tuple:

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

    for table_name, columns in db2_schema.items():
        if table_name not in db1_schema:
            db2_only.append((table_name, columns))

    return same, diffs, db1_only, db2_only

# Generate the comparison report based on the schema details
def generate_report(db1_conn: Connection, db2_conn: Connection, same: list, diffs: list, db1_only: list, db2_only: list) -> str:
    print("Generating comparison report...")

    report = ""
    db1_name = db1_conn.db.decode('ascii')
    db2_name = db2_conn.db.decode('ascii')

    if len(diffs):
        report += f"\n{len(diffs)} tables are different:\n"
        report += generate_compare_report(db1_conn, db2_conn, diffs)

    if len(db1_only):
        report += f"\n{len(db1_only)} tables exist in `{db1_name}` from {db1_conn.host} only:\n    "
        report += "\n    ".join(db1_only)

    if len(db2_only):
        report += f"\n{len(db2_only)} tables exist in `{db2_name}` from {db2_conn.host} only:\n    "
        report += "\n    ".join([tbl for tbl, _ in db2_only])

    if len(same):
        report += f"\n{len(same)} tables are equal:\n    "
        report += "\n    ".join(same)

    print("Comparison report generation completed")
    return report

# Generate comparison report for different tables
def generate_compare_report(db1_conn: Connection, db2_conn: Connection, diffs: list) -> str:
    db1_title = db1_conn.db.decode('ascii') + ' of ' + db1_conn.host
    db2_title = db2_conn.db.decode('ascii') + ' of ' + db2_conn.host
    lines = [(db1_title, db2_title)]
    
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
        
        # report += f"\n    {table_name}\n"
        lines += [(table_name,)]
        # report += f"    {db1_title[:50]:^50} | {db2_title[:50]:^50}"
        if len(deepdiff):
            lines += [(field1, field2) for field1, field2 in deepdiff]
        if len(x_new):
            lines += [(field, None) for field in x_new]
        if len(y_new):
            lines += [(None, field) for field in y_new]

    maxlen1 = len(max([str(line[0]) for line in lines if len(line)==2], key=len))
    maxlen2 = len(max([str(line[1]) for line in lines if len(line)==2], key=len))

    report = f"╔═{'═'*maxlen1}═╤═{'═'*maxlen2}═╗\n"
    titles = lines.pop(0)
    report += f"║ {titles[0]:^{maxlen1}} │ {titles[1]:^{maxlen2}} ║\n"
    for line in lines:
        if len(line) == 2:
            report += f"║ {str(line[0]):{maxlen1}} │ {str(line[1]):{maxlen2}} ║\n"
        else:
            report += f"╟─{'─'*maxlen1}─┴─{'─'*maxlen2}─╢\n"
            report += f"║ table: {str(line[0]):{maxlen1 + maxlen2 - 4}} ║\n"
            report += f"╟─{'─'*maxlen1}─┬─{'─'*maxlen2}─╢\n"
    report += f"╚═{'═'*maxlen1}═╧═{'═'*maxlen2}═╝\n"

    return report

# Generate SQL statements to make two databases consistent
def generate_sql_statements(diffs: list, db1_only: list, db2_only: list) -> str:
    sql = ""

    if len(db1_only):
        sql += "/* Drop tables only in the first database */\n"
        sql += "\n".join([f"DROP TABLE {tbl};" for tbl in db1_only])

    if len(db2_only):
        sql += "/* Create tables only in the second database */\n"
        sql += generate_create_statements(db2_only)

    if len(diffs):
        sql += "/* Alter tables different from the second database */\n"
        sql += generate_alter_statements(diffs)

    return sql

# Generate create table statements
def generate_create_statements(db2_only: list) -> str:
    sql = ""

    for tbl, columns in db2_only:
        sql += f"CREATE TABLE `{tbl}` (\n"

        for field, type, nullable, key, default, extra in columns:
            sql += f"    `{field}` {type}{'' if nullable=='YES' else ' NOT NULL'}{' PRIMARY KEY' if key=='PRI' else ''}{'' if default==None else ' DEFAULT '+default}{'' if extra=='' else ' ' + extra},\n"

        sql = sql[:-2] + ");\n"

    return sql

# Generate SQL statements for table updates
def generate_alter_statements(diffs: list) -> str:
    sql = ""
    
    for tbl, columns1, columns2 in diffs:
        sql += f"ALTER TABLE {tbl}\n"
    
        db1_only_cols = columns1 - columns2
        db2_only_cols = columns2 - columns1
        fields1 = [field for field, *_ in db1_only_cols]
        fields2 = [field for field, *_ in db2_only_cols]
    
        for field in fields1:
            if field not in fields2:
                sql += f"    DROP COLUMN `{field}`,\n"
        
        for field, type, nullable, key, default, extra in db2_only_cols:
            if field in fields1:
                sql += f"    MODIFY COLUMN `{field}` {type}{'' if nullable=='YES' else ' NOT NULL'}{' PRIMARY KEY' if key=='PRI' else ''}{'' if default==None else ' DEFAULT '+default}{'' if extra=='' else ' ' + extra},\n"
            else:
                sql += f"    ADD COLUMN `{field}` {type}{'' if nullable=='YES' else ' NOT NULL'}{' PRIMARY KEY' if key=='PRI' else ''}{'' if default==None else ' DEFAULT '+default}{'' if extra=='' else ' ' + extra},\n"
    
        sql = sql[:-2] + ';\n'
    
    return sql

def write_outputs_to_file(filename: str, content: str) -> None:
    with open(filename, 'w+', encoding="utf-8") as f:
        f.write(content)

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
