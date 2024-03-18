using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
using Sixnet.Development.Data.Database;
using Sixnet.Exceptions;

namespace Sixnet.Database.Oracle
{
    /// <summary>
    /// Defines database provider implementation for oracle
    /// </summary>
    public class OracleProvider : BaseDatabaseProvider
    {
        #region Constructor

        public OracleProvider()
        {
            queryDatabaseTablesScript = "SELECT TABLE_NAME AS \"TableName\" FROM USER_TABLES WHERE TABLE_NAME NOT LIKE '%$%' AND TABLE_NAME NOT LIKE '%LOGMNRC_%' AND TABLE_NAME NOT LIKE '%LOGMNR%' AND TABLE_NAME NOT LIKE '%SQLPLUS_%' AND TABLE_NAME!='HELP' AND TABLE_NAME!= 'REDO_DB' AND TABLE_NAME!='REDO_LOG' AND TABLE_NAME!='SCHEDULER_PROGRAM_ARGS_TBL' AND TABLE_NAME!='SCHEDULER_JOB_ARGS_TBL'";
        }

        #endregion

        #region Connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns></returns>
        public override IDbConnection GetDbConnection(DatabaseServer server)
        {
            return OracleManager.GetConnection(server);
        }

        #endregion

        #region Data command resolver

        /// <summary>
        /// Get data command resolver
        /// </summary>
        /// <returns></returns>
        protected override ISixnetDataCommandResolver GetDataCommandResolver()
        {
            return OracleManager.GetCommandResolver();
        }

        #endregion

        #region Parameter

        /// <summary>
        /// Convert data command parametes
        /// </summary>
        /// <param name="parameters">Data command parameters</param>
        /// <returns></returns>
        protected override DynamicParameters ConvertDataCommandParameters(DataCommandParameters parameters)
        {
            return parameters?.ConvertToDynamicParameters(OracleManager.CurrentDatabaseServerType);
        }

        #endregion

        #region Insert

        /// <summary>
        /// Insert data and return auto identities
        /// </summary>
        /// <param name="command">Database multiple command</param>
        /// <returns>Added data identities,Key: command id, Value: identity value</returns>
        public override Dictionary<string, TIdentity> InsertAndReturnIdentity<TIdentity>(MultipleDatabaseCommand command)
        {
            var dataCommandResolver = GetDataCommandResolver() as OracleDataCommandResolver;
            var statements = dataCommandResolver.GenerateDatabaseExecutionStatements(command);
            var identityDict = new Dictionary<string, TIdentity>();
            var dbConnection = command.Connection.DbConnection;
            foreach (var statement in statements)
            {
                var commandDefinition = GetCommandDefinition(command, statement);
                dbConnection.Execute(commandDefinition);
                if (commandDefinition.Parameters is DynamicParameters commandParameters && statement.Parameters != null)
                {
                    foreach (var parItem in statement.Parameters.Items)
                    {
                        if (parItem.Value.ParameterDirection == ParameterDirection.Output)
                        {
                            identityDict[parItem.Key] = commandParameters.Get<TIdentity>(parItem.Key);
                        }
                    }
                }
            }
            return identityDict;
        }

        /// <summary>
        /// Insert data and return auto identities
        /// </summary>
        /// <param name="command">Database multiple command</param>
        /// <returns>Added data identities,Key: command id, Value: identity value</returns>
        public override async Task<Dictionary<string, TIdentity>> InsertAndReturnIdentityAsync<TIdentity>(MultipleDatabaseCommand command)
        {
            var dataCommandResolver = GetDataCommandResolver() as OracleDataCommandResolver;
            var statements = dataCommandResolver.GenerateDatabaseExecutionStatements(command);
            var identityDict = new Dictionary<string, TIdentity>();
            var dbConnection = command.Connection.DbConnection;
            foreach (var statement in statements)
            {
                var commandDefinition = GetCommandDefinition(command, statement);
                await dbConnection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
                if (commandDefinition.Parameters is DynamicParameters commandParameters && statement.Parameters != null)
                {
                    foreach (var parItem in statement.Parameters.Items)
                    {
                        if (parItem.Value.ParameterDirection == ParameterDirection.Output)
                        {
                            identityDict[parItem.Key.LSplit(dataCommandResolver.ParameterPrefix)[0]] = commandParameters.Get<TIdentity>(parItem.Key);
                        }
                    }
                }
            }
            return identityDict;
        }

        #endregion

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public override async Task BulkInsertAsync(BulkInsertDatabaseCommand command)
        {
            BulkInsert(command);
            await Task.CompletedTask.ConfigureAwait(false);
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public override void BulkInsert(BulkInsertDatabaseCommand command)
        {
            var server = command?.Connection?.DatabaseServer;
            SixnetDirectThrower.ThrowArgNullIf(server == null, nameof(BulkInsertDatabaseCommand.Connection.DatabaseServer));
            var dataTable = command.DataTable;
            SixnetDirectThrower.ThrowArgNullIf(dataTable == null, nameof(BulkInsertDatabaseCommand.DataTable));

            var oracleBulkInsertOptions = command.BulkInsertionOptions as OracleBulkInsertionOptions;
            oracleBulkInsertOptions ??= new OracleBulkInsertionOptions();
            using (var oracleBulkCopy = new OracleBulkCopy(server?.ConnectionString))
            {
                try
                {
                    oracleBulkCopy.DestinationTableName = dataTable.TableName;
                    if (oracleBulkInsertOptions.UseTransaction)
                    {
                        oracleBulkCopy.BulkCopyOptions = OracleBulkCopyOptions.UseInternalTransaction;
                    }
                    if (!oracleBulkInsertOptions.ColumnMappings.IsNullOrEmpty())
                    {
                        oracleBulkInsertOptions.ColumnMappings.ForEach(c =>
                        {
                            c.DestinationColumn = OracleManager.FormatKeyword(c.DestinationColumn);
                            oracleBulkCopy.ColumnMappings.Add(c);
                        });
                    }
                    else
                    {
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            string destName = column.ColumnName;
                            destName = OracleManager.FormatKeyword(destName);
                            oracleBulkCopy.ColumnMappings.Add(new OracleBulkCopyColumnMapping()
                            {
                                SourceColumn = column.ColumnName,
                                DestinationColumn = destName
                            });
                        }
                    }
                    if (oracleBulkInsertOptions.BulkCopyTimeout > 0)
                    {
                        oracleBulkCopy.BulkCopyTimeout = oracleBulkInsertOptions.BulkCopyTimeout;
                    }
                    if (oracleBulkInsertOptions.BatchSize > 0)
                    {
                        oracleBulkCopy.BatchSize = oracleBulkInsertOptions.BatchSize;
                    }
                    if (oracleBulkInsertOptions.NotifyAfter > 0)
                    {
                        oracleBulkCopy.NotifyAfter = oracleBulkInsertOptions.NotifyAfter;
                    }
                    oracleBulkCopy.DestinationTableName = OracleManager.FormatKeyword(oracleBulkCopy.DestinationTableName);
                    oracleBulkCopy.WriteToServer(dataTable);
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (oracleBulkCopy?.Connection != null && oracleBulkCopy.Connection.State != ConnectionState.Closed)
                    {
                        oracleBulkCopy.Connection.Close();
                    }
                }
            }
        }

        #endregion                                                                                                                                                           
    }
}
