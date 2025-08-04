#!/bin/bash

# Start SQL Server in background
/opt/mssql/bin/sqlservr &

# Wait for SQL Server to start
echo "Waiting for SQL Server to start..."
for i in {1..50}; do
  /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -C -Q "SELECT 1" >/dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "SQL Server started successfully!"
    break
  fi
  echo "Attempt $i: SQL Server not ready yet..."
  sleep 2
done

# Check if SQL Server started successfully
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -C -Q "SELECT 1" >/dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "ERROR: SQL Server failed to start"
  exit 1
fi

# Run initialization scripts
echo "Running initialization scripts..."
for script in /mssql-init/*.sql; do
  if [ -f "$script" ]; then
    echo "Executing $script..."
    /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P $SA_PASSWORD -i "$script"
    if [ $? -eq 0 ]; then
      echo "Successfully executed $script"
    else
      echo "ERROR: Failed to execute $script"
      exit 1
    fi
  fi
done

echo "Database initialization completed!"

# Keep SQL Server running in foreground
wait
