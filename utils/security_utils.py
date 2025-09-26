def is_safe_sql(sql: str) -> bool:
    """
    Performs a basic safety check on the generated SQL query to prevent
    potentially malicious operations like INSERT, UPDATE, DELETE, DROP, ALTER, CREATE.
    Also checks for multiple semicolons which could indicate multiple statements.

    Args:
        sql (str): The SQL query string to check.

    Returns:
        bool: True if the SQL is deemed safe, False otherwise.
    """
    sql_lower = sql.lower()

    # Keywords to block regardless of position for DDL/DML operations
    unsafe_keywords = ['insert', 'update', 'delete', 'drop', 'alter', 'create']
    if any(kw in sql_lower for kw in unsafe_keywords):
        return False

    # Check for semicolon usage: only allowed if it's at the very end and only once
    # This prevents SQL injection attacks where multiple statements are chained.
    semicolon_count = sql.count(';')
    if semicolon_count > 1 or (semicolon_count == 1 and not sql.strip().endswith(';')):
        return False

    return True