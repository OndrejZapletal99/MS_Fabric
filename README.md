# MS Fabric

MS Fabric is a platform for managing, automating, and integrating data workflows, analytics, and reporting, especially in the Microsoft ecosystem. It enables seamless orchestration of data processes, including refreshing datasets, logging activities, and connecting to various services such as Power BI and SQL databases.

## 1. UDFs

This section contains User Defined Functions (UDFs) that extend the capabilities of MS Fabric by allowing custom automation and integration tasks. UDFs can be used to trigger actions, process data, and interact with external systems directly from your data workflows.

### 1.1 Single and Multiple Table Refresh

The following function demonstrates how to trigger a refresh of one or more tables in a Power BI dataset and log the action into a SQL database. It uses anonymized credentials and is suitable for presentation or template purposes.

In addition to refreshing tables, the function also logs the refresh process into the specified SQL database. This ensures that every refresh request is recorded for auditing and monitoring purposes.

```python
udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB", alias="sqlDBAlias")
@udf.function()
def RefreshTableInPowerBI(
    sqlDB: fn.FabricSqlConnection,
    workspaceid: str,
    datasetid: str,
    tablename: str,
    userId: str
) -> str:
    """
    Trigger a refresh of one or more tables in a Power BI dataset and log the action into SQL.
    Anonymized version for presentation.
    """

    timestamp = datetime.now()
    action = "RefreshTableRequest"

    # --- Acquire token (anonymized credentials) ---
    app = msal.ConfidentialClientApplication(
        client_id='YOUR_CLIENT_ID',
        client_credential='YOUR_CLIENT_SECRET',
        authority='https://login.microsoftonline.com/YOUR_TENANT_ID'
    )
    token = app.acquire_token_for_client(scopes=['https://analysis.windows.net/powerbi/api/.default'])
    if 'access_token' not in token:
        raise Exception(f"Token acquisition failed: {token.get('error_description')}")

    # --- Prepare tables ---
    table_list = [t.strip() for t in tablename.split("|") if t.strip()]
    objects = [{"table": t} for t in table_list]

    # --- Call Power BI REST API ---
    resp = requests.post(
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspaceid}/datasets/{datasetid}/refreshes",
        headers={'Authorization': f"Bearer {token['access_token']}", 'Content-Type': 'application/json'},
        json={"type": "Full", "objects": objects}
    )

    # --- Log into SQL ---
    try:
        connection = sqlDB.connect()
        cursor = connection.cursor()
        for tbl in table_list:
            cursor.execute(
                """
                INSERT INTO [SomeSchema].[SomeLogTable]
                (TimeStamp, WorkspaceId, SemanticModelId, TableName, Action, UserId)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (timestamp, workspaceid, datasetid, tbl, action, userId)
            )
        connection.commit()
    finally:
        if cursor: cursor.close()
        if connection: connection.close()

    if resp.status_code not in (200, 202):
        resp.raise_for_status()

    return resp.text
```