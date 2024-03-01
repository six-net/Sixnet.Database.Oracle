using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Database;
using Sixnet.Development.Data.Field;
using Sixnet.Development.Data.Field.Formatting;
using Sixnet.Development.Entity;
using Sixnet.Development.Queryable;
using Sixnet.Exceptions;

namespace Sixnet.Database.Oracle
{
    /// <summary>
    /// Defines command resolver for oracle
    /// </summary>
    public class OracleDataCommandResolver : BaseSixnetDataCommandResolver
    {
        #region Constructor

        public OracleDataCommandResolver()
        {
            DatabaseServerType = DatabaseServerType.Oracle;
            DefaultFieldFormatter = new OracleDefaultFieldFormatter();
            ParameterPrefix = ":";
            WrapKeywordFunc = OracleManager.FormatKeyword;
            TablePetNameKeyword = " ";
            RecursiveKeyword = "WITH";
            UseFieldForRecursive = true;
            DbTypeDefaultValues = new Dictionary<DbType, string>()
            {
                { DbType.Byte, "0" },
                { DbType.SByte, "0" },
                { DbType.Int16, "0" },
                { DbType.UInt16, "0" },
                { DbType.Int32, "0" },
                { DbType.UInt32, "0" },
                { DbType.Int64, "0" },
                { DbType.UInt64, "0" },
                { DbType.Single, "0" },
                { DbType.Double, "0" },
                { DbType.Decimal, "0" },
                { DbType.Boolean, "0" },
                { DbType.String, "''" },
                { DbType.StringFixedLength, "''" },
                { DbType.Guid, "SYS_GUID()" },
                { DbType.DateTime, "SYSTIMESTAMP" },
                { DbType.DateTime2, "SYSTIMESTAMP" },
                { DbType.DateTimeOffset, "SYSTIMESTAMP" },
                { DbType.Time, "(SYSTIMESTAMP-SYSTIMESTAMP)" }
            };
            NotParameterizationFormatterNameDict = new Dictionary<string, bool>()
            {
                { FieldFormatterNames.JSON_VALUE,true},
                { FieldFormatterNames.JSON_OBJECT,true}
            };
        }

        #endregion

        #region Get query statement

        /// <summary>
        /// Get query statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <param name="translationResult">Queryable translation result</param>
        /// <param name="queryableLocation">Queryable location</param>
        /// <returns></returns>
        protected override QueryDatabaseStatement GenerateQueryStatementCore(DataCommandResolveContext context, QueryableTranslationResult translationResult, QueryableLocation location)
        {
            var queryable = translationResult.GetOriginalQueryable();
            string sqlStatement;
            IEnumerable<ISixnetDataField> outputFields = null;
            switch (queryable.ExecutionMode)
            {
                case QueryableExecutionMode.Script:
                    sqlStatement = translationResult.GetCondition();
                    break;
                case QueryableExecutionMode.Regular:
                default:
                    // table pet name
                    var tablePetName = context.GetTablePetName(queryable, queryable.GetModelType());
                    //sort
                    var sort = translationResult.GetSort();
                    var hasSort = !string.IsNullOrWhiteSpace(sort);
                    //limit
                    var limit = GetLimitString(queryable.SkipCount, queryable.TakeCount, hasSort);
                    //combine
                    var combine = translationResult.GetCombine();
                    var hasCombine = !string.IsNullOrWhiteSpace(combine);
                    //group
                    var group = translationResult.GetGroup();
                    //having
                    var having = translationResult.GetHavingCondition();
                    //pre script output
                    var targetScript = translationResult.GetPreOutputStatement();
                    // target
                    if (string.IsNullOrWhiteSpace(targetScript))
                    {
                        //target
                        var targetStatement = GetFromTargetStatement(context, queryable, location, tablePetName);
                        outputFields = targetStatement.OutputFields;
                        //condition
                        var condition = translationResult.GetCondition(ConditionStartKeyword);
                        //join
                        var join = translationResult.GetJoin();
                        //target statement
                        targetScript = $"{targetStatement.Script}{join}{condition}{group}{having}";
                    }
                    else
                    {
                        targetScript = $"{targetScript}{group}{having}";
                        outputFields = translationResult.GetPreOutputFields();
                    }

                    // output fields
                    if (outputFields.IsNullOrEmpty() || !queryable.SelectedFields.IsNullOrEmpty())
                    {
                        outputFields = SixnetDataManager.GetQueryableFields(DatabaseServerType, queryable.GetModelType(), queryable, context.IsRootQueryable(queryable));
                    }
                    var outputFieldString = FormatFieldsString(context, queryable, location, FieldLocation.Output, outputFields);
                    //pre script
                    var preScript = GetPreScript(context, location);
                    //statement
                    sqlStatement = $"SELECT{GetDistinctString(queryable)} {outputFieldString} FROM {targetScript}{sort}{limit}";
                    switch (queryable.OutputType)
                    {
                        case QueryableOutputType.Count:
                            sqlStatement = hasCombine
                                ? $"{preScript}SELECT COUNT(1) FROM (({sqlStatement}){combine}){TablePetNameKeyword}{tablePetName}"
                                : $"{preScript}SELECT COUNT(1) FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}";
                            break;
                        case QueryableOutputType.Predicate:
                            sqlStatement = hasCombine
                                ? $"{preScript}SELECT CASE WHEN EXISTS(({sqlStatement}){combine}) THEN 1 ELSE 0 END FROM DUAL"
                                : $"{preScript}SELECT CASE WHEN EXISTS({sqlStatement}) THEN 1 ELSE 0 END FROM DUAL";
                            break;
                        default:
                            sqlStatement = hasCombine
                            ? $"{preScript}({sqlStatement}){combine}"
                            : $"{preScript}{sqlStatement}";
                            break;
                    }
                    break;
            }

            //parameters
            var parameters = context.GetParameters();

            //log script
            if (location == QueryableLocation.Top)
            {
                LogScript(sqlStatement, parameters);
            }

            return QueryDatabaseStatement.Create(sqlStatement, parameters, outputFields);
        }

        #endregion

        #region Get insert statement

        /// <summary>
        /// Get insert statements
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<ExecutionDatabaseStatement> GenerateInsertStatements(DataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            var fields = SixnetDataManager.GetInsertableFields(DatabaseServerType, entityType);
            var fieldCount = fields.GetCount();
            var insertFields = new List<string>(fieldCount);
            var insertValues = new List<string>(fieldCount);
            EntityField autoIncrementField = null;
            EntityField splitField = null;
            dynamic splitValue = default;

            foreach (var field in fields)
            {
                if (field.InRole(FieldRole.Increment))
                {
                    autoIncrementField ??= field;
                    if (!autoIncrementField.InRole(FieldRole.PrimaryKey) && field.InRole(FieldRole.PrimaryKey)) // get first primary key field
                    {
                        autoIncrementField = field;
                    }
                    continue;
                }
                // fields
                insertFields.Add(WrapKeywordFunc(field.FieldName));
                // values
                var insertValue = command.FieldsAssignment.GetNewValue(field.PropertyName);
                insertValues.Add(FormatInsertValueField(context, command.Queryable, insertValue));
                // split value
                if (field.InRole(FieldRole.SplitValue))
                {
                    splitValue = insertValue;
                    splitField = field;
                }
            }

            SixnetDirectThrower.ThrowNotSupportIf(autoIncrementField != null && splitField != null, $"Not support auto increment field for split table:{entityType.Name}");

            if (splitField != null)
            {
                dataCommandExecutionContext.SetSplitValues(new List<dynamic>(1) { splitValue });
            }
            var tableNames = dataCommandExecutionContext.GetTableNames();
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");

            // incr field
            var incrementFieldScript = string.Empty;
            if (autoIncrementField != null)
            {
                var idOutputParameterName = FormatParameterName(command.Id);
                incrementFieldScript = $" RETURNING {WrapKeywordFunc(autoIncrementField.FieldName)} INTO {idOutputParameterName}";
                context.AddOutputParameter(command.Id, autoIncrementField.DataType.GetDbType());
            }

            var scriptTemplate = $"INSERT INTO {{0}} ({string.Join(",", insertFields)}) VALUES ({string.Join(",", insertValues)}){incrementFieldScript}";

            var statements = new List<ExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(new ExecutionDatabaseStatement()
                {
                    Script = string.Format(scriptTemplate, WrapKeywordFunc(tableName)),
                    ScriptType = GetCommandType(command),
                    Parameters = context.GetParameters(),
                    MustAffectData = true
                });
            }

            return statements;
        }

        #endregion

        #region Get update statement

        /// <summary>
        /// Get update statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<ExecutionDatabaseStatement> GenerateUpdateStatements(DataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            SixnetException.ThrowIf(command?.FieldsAssignment?.NewValues.IsNullOrEmpty() ?? true, "No set update field");

            #region translate

            var translationResult = Translate(context);
            var join = translationResult?.GetJoin();
            var preScripts = context.GetPreScripts();

            #endregion

            #region script 

            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();

            var tableNames = dataCommandExecutionContext.GetTableNames(command);
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");

            var tablePetName = command.Queryable == null ? context.GetNewTablePetName() : context.GetDefaultTablePetName(command.Queryable);
            var newValues = command.FieldsAssignment.NewValues;
            var updateSetArray = new List<string>();
            foreach (var newValueItem in newValues)
            {
                var newValue = newValueItem.Value;
                var propertyName = newValueItem.Key;
                var updateField = SixnetDataManager.GetField(dataCommandExecutionContext.Server.ServerType, command.GetEntityType(), PropertyField.Create(propertyName)) as PropertyField;
                SixnetDirectThrower.ThrowSixnetExceptionIf(updateField == null, $"Not found field:{propertyName}");
                var fieldFormattedName = WrapKeywordFunc(updateField.FieldName);
                var newValueExpression = FormatUpdateValueField(context, command, newValue);
                updateSetArray.Add($"{fieldFormattedName}={newValueExpression}");
            }

            string scriptTemplate;
            if (preScripts.IsNullOrEmpty() && string.IsNullOrWhiteSpace(join))
            {
                var condition = translationResult?.GetCondition(ConditionStartKeyword);
                scriptTemplate = $"UPDATE {{0}}{TablePetNameKeyword}{tablePetName} SET {string.Join(",", updateSetArray)}{condition}";
            }
            else
            {
                var queryStatement = GenerateQueryStatementCore(context, translationResult, QueryableLocation.UsingSource);
                var updateTablePetName = "UTB";
                var joinItems = FormatWrapJoinPrimaryKeys(context, command.Queryable, command.GetEntityType(), tablePetName, tablePetName, updateTablePetName);
                scriptTemplate = $"MERGE INTO {{0}}{TablePetNameKeyword}{tablePetName} USING ({queryStatement.Script}) {updateTablePetName} ON ({string.Join(" AND ", joinItems)}) WHEN MATCHED THEN UPDATE SET {string.Join(",", updateSetArray)}";
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new DataCommandParameters();
            parameters.Union(context.GetParameters());

            var statements = new List<ExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(new ExecutionDatabaseStatement()
                {
                    Script = string.Format(scriptTemplate, WrapKeywordFunc(tableName)),
                    ScriptType = GetCommandType(command),
                    Parameters = parameters,
                    MustAffectData = true,
                    HasPreScript = !preScripts.IsNullOrEmpty()
                });
            }

            #endregion

            return statements;
        }

        #endregion

        #region Get delete statement

        /// <summary>
        /// Get delete statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<ExecutionDatabaseStatement> GenerateDeleteStatements(DataCommandResolveContext context)
        {
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var command = dataCommandExecutionContext.Command;

            #region translate

            var translationResult = Translate(context);
            var join = translationResult?.GetJoin();
            var preScript = FormatPreScript(context);
            var preScripts = context.GetPreScripts();

            #endregion

            #region script

            var tableNames = dataCommandExecutionContext.GetTableNames(command);
            var entityType = dataCommandExecutionContext.Command.GetEntityType();

            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");
            var tablePetName = command.Queryable == null ? context.GetNewTablePetName() : context.GetDefaultTablePetName(command.Queryable);

            string scriptTemplate;
            if (preScripts.IsNullOrEmpty() && string.IsNullOrWhiteSpace(join))
            {
                var condition = translationResult?.GetCondition(ConditionStartKeyword);
                scriptTemplate = $"DELETE {{0}}{TablePetNameKeyword}{tablePetName}{condition}";
            }
            else
            {
                var primaryKeyFields = SixnetDataManager.GetFields(DatabaseServerType, entityType, SixnetEntityManager.GetPrimaryKeyFields(entityType)).ToList();
                SixnetException.ThrowIf(primaryKeyFields.IsNullOrEmpty(), $"{entityType.FullName} not set primary key");

                var primaryKeyString = string.Join("||", primaryKeyFields.Select(pk => FormatField(context, command.Queryable, pk, QueryableLocation.Top, FieldLocation.Criterion, tablePetName: tablePetName)));
                var queryStatement = GenerateQueryStatementCore(context, translationResult, QueryableLocation.UsingSource);
                scriptTemplate = $"DELETE FROM {{0}}{TablePetNameKeyword}{tablePetName} WHERE {primaryKeyString} IN (SELECT {primaryKeyString} FROM ({queryStatement.Script}){TablePetNameKeyword}{tablePetName})";
            }

            var parameters = ConvertParameter(command.ScriptParameters) ?? new DataCommandParameters();
            parameters.Union(context.GetParameters());

            var statements = new List<ExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(new ExecutionDatabaseStatement()
                {
                    Script = string.Format(scriptTemplate, WrapKeywordFunc(tableName)),
                    ScriptType = GetCommandType(command),
                    MustAffectData = true,
                    Parameters = parameters,
                    HasPreScript = !string.IsNullOrWhiteSpace(preScript)
                });
            }

            #endregion

            return statements;
        }

        #endregion

        #region Get create table statements

        /// <summary>
        /// Get create table statements
        /// </summary>
        /// <param name="migrationCommand">Migration command</param>
        /// <returns></returns>
        protected override List<ExecutionDatabaseStatement> GetCreateTableStatements(MigrationDatabaseCommand migrationCommand)
        {
            var migrationInfo = migrationCommand.MigrationInfo;
            if (migrationInfo?.NewTables.IsNullOrEmpty() ?? true)
            {
                return new List<ExecutionDatabaseStatement>(0);
            }
            var newTables = migrationInfo.NewTables;
            var statements = new List<ExecutionDatabaseStatement>();
            var options = migrationCommand.MigrationInfo;
            foreach (var newTableInfo in newTables)
            {
                if (newTableInfo?.EntityType == null || (newTableInfo?.TableNames.IsNullOrEmpty() ?? true))
                {
                    continue;
                }
                var entityType = newTableInfo.EntityType;
                var entityConfig = SixnetEntityManager.GetEntityConfiguration(entityType);
                SixnetDirectThrower.ThrowSixnetExceptionIf(entityConfig == null, $"Get entity config failed for {entityType.Name}");

                var newFieldScripts = new List<string>();
                var primaryKeyNames = new List<string>();
                foreach (var field in entityConfig.AllFields)
                {
                    var dataField = SixnetDataManager.GetField(OracleManager.CurrentDatabaseServerType, entityType, field.Value);
                    if (dataField is EntityField dataEntityField)
                    {
                        var dataFieldName = dataEntityField.FieldName;
                        newFieldScripts.Add($"{dataFieldName}{GetSqlDataType(dataEntityField, options)}{GetFieldNullable(dataEntityField, options)}{GetSqlDefaultValue(dataEntityField, options)}");
                        if (dataEntityField.InRole(FieldRole.PrimaryKey))
                        {
                            primaryKeyNames.Add($"{dataFieldName}");
                        }
                    }
                }
                foreach (var tableName in newTableInfo.TableNames)
                {
                    var realTableName = OracleManager.OracleOptions.Uppercase ? tableName.ToUpper() : tableName;
                    var createTableStatement = new ExecutionDatabaseStatement()
                    {
                        Script = $"DECLARE TB_EX NUMBER; BEGIN SELECT COUNT(*) INTO TB_EX FROM user_tables WHERE table_name = '{realTableName}'; IF TB_EX =0 THEN EXECUTE IMMEDIATE 'CREATE TABLE {realTableName} ({string.Join(",", newFieldScripts)}{(primaryKeyNames.IsNullOrEmpty() ? "" : $", CONSTRAINT PK_{realTableName} PRIMARY KEY ({string.Join(",", primaryKeyNames)})")})'; END IF; END;"
                    };
                    statements.Add(createTableStatement);

                    // Log script
                    LogExecutionStatement(createTableStatement);
                }
            }
            return statements;
        }

        #endregion

        #region Get combine operator

        /// <summary>
        /// Get combine operator
        /// </summary>
        /// <param name="combineType">Combine type</param>
        /// <returns>Return combine operator</returns>
        protected override string GetCombineOperator(CombineType combineType)
        {
            return combineType switch
            {
                CombineType.UnionAll => " UNION ALL ",
                CombineType.Union => " UNION ",
                CombineType.Except => " MINUS ",
                CombineType.Intersect => " INTERSECT ",
                _ => throw new InvalidOperationException($"{DatabaseServerType} not support {combineType}"),
            };
        }

        #endregion

        #region Get limit string

        /// <summary>
        /// Get limit string
        /// </summary>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="takeNum">Take num</param>
        /// <param name="hasSort">Whether has sort</param>
        /// <returns></returns>
        protected override string GetLimitString(int offsetNum, int takeNum, bool hasSort)
        {
            if (takeNum < 1)
            {
                return string.Empty;
            }
            if (offsetNum < 0)
            {
                offsetNum = 0;
            }
            return $" OFFSET {offsetNum} ROWS FETCH NEXT {takeNum} ROWS ONLY";

        }

        #endregion

        #region Get field sql data type

        /// <summary>
        /// Get sql data type
        /// </summary>
        /// <param name="field">Field</param>
        /// <returns></returns>
        protected override string GetSqlDataType(EntityField field, MigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            var dbTypeName = "";
            if (!string.IsNullOrWhiteSpace(field.DbType))
            {
                dbTypeName = field.DbType;
            }
            else
            {
                var dbType = field.DataType.GetDbType();
                var length = field.Length;
                var precision = field.Precision;
                var notFixedLength = options.NotFixedLength || field.HasDbFeature(FieldDbFeature.NotFixedLength);
                static int getCharLength(int flength, int defLength) => flength < 1 ? defLength : flength;
                switch (dbType)
                {
                    case DbType.AnsiString:
                        dbTypeName = $"VARCHAR2({getCharLength(length, DefaultCharLength)} char)";
                        break;
                    case DbType.AnsiStringFixedLength:
                        dbTypeName = $"NVARCHAR2({getCharLength(length, DefaultCharLength)})";
                        break;
                    case DbType.Binary:
                        dbTypeName = $"RAW({getCharLength(length, DefaultCharLength)})";
                        break;
                    case DbType.Boolean:
                        dbTypeName = "NUMBER(1)";
                        break;
                    case DbType.Byte:
                    case DbType.SByte:
                        dbTypeName = "NUMBER(3)";
                        break;
                    case DbType.Date:
                        dbTypeName = "DATE";
                        break;
                    case DbType.DateTime:
                    case DbType.DateTime2:
                        dbTypeName = "TIMESTAMP(7)";
                        break;
                    case DbType.DateTimeOffset:
                        dbTypeName = "TIMESTAMP(7) WITH TIME ZONE";
                        break;
                    case DbType.Decimal:
                    case DbType.Currency:
                        dbTypeName = $"DECIMAL({(length < 1 ? DefaultDecimalLength : length)}, {(precision < 0 ? DefaultDecimalPrecision : precision)})";
                        break;
                    case DbType.Double:
                        dbTypeName = "BINARY_DOUBLE";
                        break;
                    case DbType.Guid:
                        dbTypeName = "RAW(16)";
                        break;
                    case DbType.Int16:
                    case DbType.UInt16:
                        dbTypeName = "NUMBER(5)";
                        break;
                    case DbType.Int32:
                    case DbType.UInt32:
                        dbTypeName = "NUMBER(10)";
                        break;
                    case DbType.Int64:
                    case DbType.UInt64:
                        dbTypeName = "NUMBER(20)";
                        break;
                    case DbType.Single:
                        dbTypeName = "BINARY_FLOAT";
                        break;
                    case DbType.String:
                        length = getCharLength(length, DefaultCharLength);
                        dbTypeName = length > 2000
                            ? (notFixedLength ? "CLOB" : "NCLOB")
                            : (notFixedLength
                                ? $"VARCHAR2({length} char)"
                                : $"NVARCHAR2({length})");
                        break;
                    case DbType.StringFixedLength:
                        dbTypeName = $"NVARCHAR2({getCharLength(length, DefaultCharLength)})";
                        break;
                    case DbType.Time:
                        dbTypeName = $"INTERVAL DAY(8) TO SECOND(7)";
                        break;
                    case DbType.Xml:
                        dbTypeName = "CLOB";
                        break;
                    default:
                        throw new NotSupportedException(dbType.ToString());
                }
            }
            return $" {dbTypeName}";
        }

        #endregion
    }
}
