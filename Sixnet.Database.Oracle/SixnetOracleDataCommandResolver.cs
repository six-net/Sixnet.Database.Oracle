using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

using Sixnet.Development.Data;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
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
    public partial class SixnetOracleDataCommandResolver : SixnetBaseDataCommandResolver
    {
        #region Constructor

        public SixnetOracleDataCommandResolver()
        {
            DatabaseType = SixnetDatabaseType.Oracle;
            DefaultFieldFormatter = new SixnetOracleDefaultFieldFormatter();
            ParameterPrefix = ":";
            KeywordPrefix = "\"";
            KeywordSuffix = "\"";
            TablePetNameKeyword = " ";
            RecursiveKeyword = "WITH";
            UseFieldForRecursive = true;
            MaxIdentifierLength = 128;
            DisableDefaultPrimaryKeyAsc = true;
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
                { SixnetFieldFormatterNames.JSON_VALUE,true},
                { SixnetFieldFormatterNames.JSON_OBJECT,true}
            };
        }

        #endregion

        #region Data access

        #region Get query statement

        /// <summary>
        /// Get query statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <param name="translationResult">Queryable translation result</param>
        /// <param name="queryableLocation">Queryable location</param>
        /// <returns></returns>
        protected override SixnetQueryDatabaseStatement GenerateQueryStatementCore(SixnetDataCommandResolveContext context, SixnetQueryableTranslationResult translationResult, SixnetQueryableLocation location)
        {
            var queryable = translationResult.GetOriginalQueryable();
            string sqlStatement;
            IEnumerable<ISixnetField> outputFields = null;
            switch (queryable.Info.ExecutionMode)
            {
                case SixnetQueryableExecutionMode.Script:
                    sqlStatement = translationResult.GetCondition();
                    break;
                case SixnetQueryableExecutionMode.Regular:
                default:
                    // table pet name
                    var tablePetName = context.GetTablePetName(queryable, queryable.GetModelType());
                    //sort
                    var sort = translationResult.GetSort();
                    var hasSort = !string.IsNullOrWhiteSpace(sort);
                    //limit
                    var limit = GetLimitString(queryable.Info.SkipCount, queryable.Info.TakeCount, hasSort);
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
                    if (outputFields.IsNullOrEmpty() || !queryable.Info.SelectedFields.IsNullOrEmpty())
                    {
                        outputFields = SixnetDataManager.GetQueryableFields(DatabaseType, queryable.GetModelType(), queryable, context.IsRootQueryable(queryable));
                    }
                    var outputFieldString = FormatFieldsString(context, queryable, location, SixnetFieldLocation.Output, outputFields);
                    //pre script
                    var preScript = GetPreScript(context, location);
                    //statement
                    sqlStatement = $"SELECT{GetDistinctString(queryable)} {outputFieldString} FROM {targetScript}{sort}{limit}";
                    switch (queryable.Info.OutputType)
                    {
                        case SixnetQueryableOutputType.Count:
                            sqlStatement = hasCombine
                                ? hasSort
                                    ? $"{preScript}SELECT COUNT(1) FROM ((SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}){TablePetNameKeyword}{tablePetName}"
                                    : $"{preScript}SELECT COUNT(1) FROM (({sqlStatement}){combine}){TablePetNameKeyword}{tablePetName}"
                                : $"{preScript}SELECT COUNT(1) FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}";
                            break;
                        case SixnetQueryableOutputType.Predicate:
                            sqlStatement = hasCombine
                                ? hasSort
                                    ? $"{preScript}SELECT CASE WHEN EXISTS((SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}) THEN 1 ELSE 0 END FROM DUAL"
                                    : $"{preScript}SELECT CASE WHEN EXISTS(({sqlStatement}){combine}) THEN 1 ELSE 0 END FROM DUAL"
                                : $"{preScript}SELECT CASE WHEN EXISTS({sqlStatement}) THEN 1 ELSE 0 END FROM DUAL";
                            break;
                        default:
                            sqlStatement = hasCombine
                            ? hasSort
                                ? $"{preScript}(SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}"
                                : $"{preScript}({sqlStatement}){combine}"
                            : $"{preScript}{sqlStatement}";
                            break;
                    }
                    break;
            }

            //parameters
            var parameters = context.GetParameters();

            return SixnetQueryDatabaseStatement.Create(DatabaseType, location, sqlStatement, parameters, outputFields);
        }

        #endregion

        #region Get insert statement

        /// <summary>
        /// Get insert statements
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GenerateInsertStatements(SixnetDataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            var fields = SixnetDataManager.GetInsertableFields(DatabaseType, entityType);
            var fieldCount = fields.GetCount();
            var insertFields = new List<string>(fieldCount);
            var insertValues = new List<string>(fieldCount);
            SixnetDataField autoIncrementField = null;
            SixnetDataField splitField = null;
            dynamic splitValue = default;

            foreach (var field in fields)
            {
                if (field.InRole(SixnetFieldRole.Increment))
                {
                    autoIncrementField ??= field;
                    if (!autoIncrementField.InRole(SixnetFieldRole.PrimaryKey) && field.InRole(SixnetFieldRole.PrimaryKey)) // get first primary key field
                    {
                        autoIncrementField = field;
                    }
                    if (!SixnetDataManager.AllowInsertIncrementField(context.DataCommandExecutionContext))
                    {
                        continue;
                    }
                }
                // fields
                insertFields.Add(FormatAndWrapObjectName(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                // values
                var insertValue = command.FieldsAssignment.GetNewValue(field.PropertyName);
                insertValues.Add(FormatInsertValueField(context, command.Queryable, insertValue));
                // split value
                if (field.InRole(SixnetFieldRole.SplitValue))
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
                incrementFieldScript = $" RETURNING {FormatAndWrapObjectName(autoIncrementField.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column)} INTO {idOutputParameterName}";
                context.AddOutputParameter(command.Id, autoIncrementField.GetDataType().GetDbType());
            }

            var scriptTemplate = $"INSERT INTO {{0}} ({string.Join(",", insertFields)}) VALUES ({string.Join(",", insertValues)}){incrementFieldScript}";

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(SixnetExecutionDatabaseStatement.Create(DatabaseType, data =>
                {
                    data.Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName));
                    data.ScriptType = GetCommandType(command);
                    data.Parameters = context.GetParameters();
                    data.MustAffectData = true;
                }));
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
        protected override List<SixnetExecutionDatabaseStatement> GenerateUpdateStatements(SixnetDataCommandResolveContext context)
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
                var updateField = SixnetDataManager.GetField(dataCommandExecutionContext.Server.DatabaseType, command.GetEntityType(), SixnetDataField.Create(propertyName)) as SixnetDataField;
                SixnetDirectThrower.ThrowSixnetExceptionIf(updateField == null, $"Not found field:{propertyName}");
                var fieldFormattedName = FormatAndWrapObjectName(updateField.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column);
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
                var queryStatement = GenerateQueryStatementCore(context, translationResult, SixnetQueryableLocation.UsingSource);
                var updateTablePetName = "UTB";
                var joinItems = FormatWrapJoinPrimaryKeys(context, command.Queryable, command.GetEntityType(), tablePetName, tablePetName, updateTablePetName);
                scriptTemplate = $"MERGE INTO {{0}}{TablePetNameKeyword}{tablePetName} USING ({queryStatement.Script}) {updateTablePetName} ON ({string.Join(" AND ", joinItems)}) WHEN MATCHED THEN UPDATE SET {string.Join(",", updateSetArray)}";
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new SixnetDataCommandParameters();
            parameters.Union(context.GetParameters());

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(SixnetExecutionDatabaseStatement.Create(DatabaseType, data => 
                {
                    data.Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName));
                    data.ScriptType = GetCommandType(command);
                    data.Parameters = parameters;
                    data.MustAffectData = true;
                    data.HasPreScript = !preScripts.IsNullOrEmpty();
                }));
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
        protected override List<SixnetExecutionDatabaseStatement> GenerateDeleteStatements(SixnetDataCommandResolveContext context)
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
                var primaryKeyFields = SixnetDataManager.GetFields(DatabaseType, entityType, SixnetEntityManager.GetPrimaryKeyFields(entityType)).ToList();
                SixnetException.ThrowIf(primaryKeyFields.IsNullOrEmpty(), $"{entityType.FullName} not set primary key");

                var primaryKeyString = string.Join("||", primaryKeyFields.Select(pk => FormatField(context, command.Queryable, pk, SixnetQueryableLocation.Top, SixnetFieldLocation.Criterion, tablePetName: tablePetName)));
                var queryStatement = GenerateQueryStatementCore(context, translationResult, SixnetQueryableLocation.UsingSource);
                scriptTemplate = $"DELETE FROM {{0}}{TablePetNameKeyword}{tablePetName} WHERE {primaryKeyString} IN (SELECT {primaryKeyString} FROM ({queryStatement.Script}){TablePetNameKeyword}{tablePetName})";
            }

            var parameters = ConvertParameter(command.ScriptParameters) ?? new SixnetDataCommandParameters();
            parameters.Union(context.GetParameters());

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(SixnetExecutionDatabaseStatement.Create(DatabaseType, data => 
                {
                    data.Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName));
                    data.ScriptType = GetCommandType(command);
                    data.MustAffectData = true;
                    data.Parameters = parameters;
                    data.HasPreScript = !string.IsNullOrWhiteSpace(preScript);
                }));
            }

            #endregion

            return statements;
        }

        #endregion

        #endregion

        #region Migration

        #region Get create table statements

        protected override SixnetDatabaseScriptInfo GetCreateTableScripts(SixnetGetCreateTableDefineScriptParameter parameter)
        {
            var tableName = FormatObjectName(parameter.Table);
            var columnInfo = parameter.ColumnDefineInfo;
            var fields = columnInfo.ColumnScripts;
            var parimaryKeys = columnInfo.PrimaryKeys;
            var migrationInfo = parameter.MigrationInfo;
            var script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_TABLES
    WHERE OWNER = '{parameter.Table.SchemaName}'
      AND TABLE_NAME = '{tableName.Name}';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE TABLE {WrapObjectName(tableName).Name} ({string.Join(",", fields)}{(parimaryKeys.IsNullOrEmpty() ? "" : ", PRIMARY KEY (" + string.Join(",", parimaryKeys) + ")")})';
    END IF;
END;";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>() { script }
            };
        }

        #endregion

        #region Get delete all table statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllTableScripts(SixnetDeleteAllTableParameter parameter)
        {
            var schema = parameter.Schema;
            var command = parameter.Command;
            var sql = $@"
SELECT
    'DROP TABLE ""' 
    || OWNER 
    || '"".""' 
    || TABLE_NAME 
    || '"" CASCADE CONSTRAINTS;' || CHR(13)
FROM ALL_TABLES
WHERE OWNER = '{schema}'
AND TABLE_NAME NOT LIKE '##%';
";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = command.Connection.DbConnection.Query<string>(sql, transaction: command.Connection.Transaction.DbTransaction)?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get rename table statements

        /// <summary>
        /// Get rename table statements
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetRenameTableScripts(SixnetRenameTableParameter parameter)
        {
            var oldFormattedTableName = FormatObjectName(parameter.CurrentTableName);
            var newFormattedTableName = FormatObjectName(parameter.NewTableName);
            var script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_TABLES
    WHERE TABLE_NAME = '{oldFormattedTableName.Name}'
      AND OWNER = '{oldFormattedTableName.SchemaName}';

    IF v_count > 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE {WrapObjectName(oldFormattedTableName)} RENAME TO {WrapObjectName(newFormattedTableName).Name}';
    END IF;
END;";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    script
                }
            };
        }

        #endregion


        #region Get add filed statements

        /// <summary>
        /// Get add field scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetAddFieldScripts(SixnetAddFieldParameter parameter)
        {
            var table = parameter.Table;
            var fields = parameter.Fields;
            var command = parameter.Command;
            var formattedTableName = FormatObjectName(table);
            var scripts = new List<string>();
            foreach (var field in fields)
            {
                var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                var script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = '{formattedTableName.Name}'
      AND OWNER = '{formattedTableName.SchemaName}'
      AND COLUMN_NAME = '{dataFieldName.Name}';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE {WrapObjectName(formattedTableName)} ADD {WrapObjectName(dataFieldName).Name} {GetFieldDefinition(field, command.MigrationInfo)}';
    END IF;
END;";
                scripts.Add(script);
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion

        #region Get delete filed statements

        protected override SixnetDatabaseScriptInfo GetDeleteFieldScripts(SixnetDeleteFieldParameter parameter)
        {
            var scripts = new List<string>();
            var table = parameter.Table;
            var fields = parameter.Fields;
            var formattedTableName = FormatObjectName(table);
            foreach (var field in fields)
            {
                var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                var script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = '{formattedTableName.Name}'
      AND OWNER = '{formattedTableName.SchemaName}'
      AND COLUMN_NAME = '{dataFieldName.Name}';

    IF v_count > 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE {WrapObjectName(formattedTableName)} DROP COLUMN {WrapObjectName(dataFieldName).Name}';
    END IF;
END;";
                scripts.Add(script);
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion

        #region Get update field statements 

        /// <summary>
        /// Get update field scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetUpdateFieldScripts(SixnetUpdateFieldParameter parameter)
        {
            var scripts = new List<string>();
            var table = parameter.Table;
            var fields = parameter.Fields;
            var command = parameter.Command;
            var formattedTableName = FormatObjectName(table);
            foreach (var fieldItem in fields)
            {
                var field = fieldItem.Value;
                var nowFieldName = fieldItem.Key;
                var nowFormatedFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(nowFieldName, SixnetDatabaseObjectType.Column));
                var newFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                var script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = '{formattedTableName.Name}'
      AND OWNER = '{formattedTableName.SchemaName}'
      AND COLUMN_NAME = '{nowFormatedFieldName.Name}';

    IF v_count > 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE {WrapObjectName(formattedTableName)} MODIFY {WrapObjectName(nowFormatedFieldName).Name} {GetFieldDefinition(field, command.MigrationInfo)}';
    END IF;
END;";

                scripts.Add(script);
                if (!string.Equals(nowFormatedFieldName.Name, newFieldName.Name, StringComparison.OrdinalIgnoreCase))
                {
                    script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = '{formattedTableName.Name}'
      AND OWNER = '{formattedTableName.SchemaName}'
      AND COLUMN_NAME = '{nowFormatedFieldName.Name}';

    IF v_count > 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE {WrapObjectName(formattedTableName)} RENAME COLUMN {WrapObjectName(nowFormatedFieldName).Name} TO {WrapObjectName(newFieldName).Name}';
    END IF;
END;";
                    scripts.Add(script);
                }
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion


        #region Add foreign key

        /// <summary>
        /// Get add foreign key scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetAddForeignKeyScripts(SixnetAddForeignKeyParameter parameter)
        {
            var foreignKeyInfo = parameter.ForeignKeyInfo;

            var sourceFieldName = FormatObjectName(foreignKeyInfo.SourceField);
            var formattedSourceTableName = FormatObjectName(foreignKeyInfo.SourceTable);
            var wrapedSourceTableName = FormatAndWrapObjectName(foreignKeyInfo.SourceTable);

            var referenceFieldName = FormatAndWrapObjectName(foreignKeyInfo.ReferenceField);
            var referenceTableName = FormatAndWrapObjectName(foreignKeyInfo.ReferenceTable);

            var constraintName = GetForeignKeyName(formattedSourceTableName, sourceFieldName);

            var script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_CONSTRAINTS
    WHERE TABLE_NAME = '{formattedSourceTableName.Name}'
      AND OWNER = '{formattedSourceTableName.SchemaName}'
      AND CONSTRAINT_NAME = '{constraintName.Name}'
      AND CONSTRAINT_TYPE = 'R';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE {wrapedSourceTableName}
             ADD CONSTRAINT {WrapObjectName(constraintName).Name}
             FOREIGN KEY ({WrapObjectName(sourceFieldName).Name})
             REFERENCES {referenceTableName} ({referenceFieldName})';
    END IF;
END;";

            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    script
                }
            };
        }

        #endregion

        #region Delete foreign key

        /// <summary>
        /// Get delete foreign key scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public override SixnetDatabaseScriptInfo GetDeleteForeignKeyScripts(SixnetDeleteForeignKeyParameter parameter)
        {
            var foreignKeyInfo = parameter.ForeignKeyInfo;
            var sourceFieldName = FormatObjectName(foreignKeyInfo.SourceField);
            var formattedSourceTableName = FormatObjectName(foreignKeyInfo.SourceTable);
            var wrapedSourceTableName = FormatAndWrapObjectName(foreignKeyInfo.SourceTable);
            var constraintName = GetForeignKeyName(formattedSourceTableName, sourceFieldName);

            var script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_CONSTRAINTS
    WHERE TABLE_NAME = '{formattedSourceTableName.Name}'
      AND OWNER = '{formattedSourceTableName.SchemaName}'
      AND CONSTRAINT_NAME = '{constraintName.Name}'
      AND CONSTRAINT_TYPE = 'R';

    IF v_count > 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE {WrapObjectName(formattedSourceTableName)} DROP CONSTRAINT {WrapObjectName(constraintName).Name}';
    END IF;
END;";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    script
                }
            };
        }

        /// <summary>
        /// Get delete all foreign key scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetDeleteAllForeignKeyScripts(SixnetDeleteAllForeignKeyParameter parameter)
        {
            var sql = $@"
SELECT
    'ALTER TABLE ""' 
    || OWNER 
    || '"".""' 
    || TABLE_NAME 
    || '"" DROP CONSTRAINT ""' 
    || CONSTRAINT_NAME 
    || '"";'
FROM ALL_CONSTRAINTS
WHERE OWNER = '{parameter.Schema}' AND CONSTRAINT_TYPE = 'R';
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts.ToList()
            };
        }

        #endregion


        #region Add index

        /// <summary>
        /// Get add index scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetAddIndexScripts(SixnetAddIndexParameter parameter)
        {
            var indexInfo = parameter.IndexInfo;
            var formattedTableName = FormatObjectName(indexInfo.Table);
            var indexDefine = GetIndexDefine(indexInfo);
            var indexName = indexDefine.Item1;
            var indexFieldStrings = indexDefine.Item2;

            var script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_INDEXES
    WHERE OWNER = '{formattedTableName.SchemaName}'
      AND TABLE_NAME = '{formattedTableName.Name}'
      AND INDEX_NAME = '{indexName.Name}';

    IF v_count = 0 THEN
        EXECUTE IMMEDIATE
            'CREATE {(indexInfo.Unique ? "UNIQUE " : "")}INDEX {WrapObjectName(indexName).Name}
             ON {WrapObjectName(formattedTableName)} ({string.Join(",", indexFieldStrings)})';
    END IF;
END;";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    script
                }
            };
        }

        #endregion

        #region Delete index

        /// <summary>
        /// Get delete index scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetDeleteIndexScripts(SixnetDeleteIndexParameter parameter)
        {
            var indexInfo = parameter.IndexInfo;
            var indexName = GetIndexDefine(indexInfo).Item1;
            var formattedTableName = FormatObjectName(indexInfo.Table);

            var script = $@"
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM ALL_INDEXES
    WHERE OWNER = '{formattedTableName.SchemaName}'
      AND TABLE_NAME = '{formattedTableName.Name}'
      AND INDEX_NAME = '{indexName.Name}';

    IF v_count > 0 THEN
        EXECUTE IMMEDIATE
            'DROP INDEX {formattedTableName.SchemaName}.{WrapObjectName(indexName).Name}';
    END IF;
END;";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    script
                }
            };
        }

        #endregion


        #region Get delete all view statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllViewScripts(SixnetDeleteAllViewParameter parameter)
        {
            var sql = $@"
SELECT
    'DROP VIEW ""' 
    || OWNER 
    || '"".""' 
    || VIEW_NAME 
    || '"";' || CHR(13)
FROM ALL_VIEWS
WHERE OWNER = '{parameter.Schema}';
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get delete all function statements

        /// <summary>
        /// Get delete all function statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetDeleteAllFunctionScripts(SixnetDeleteAllFunctionParameter parameter)
        {
            var sql = $@"
SELECT
    'DROP FUNCTION ""' 
    || OWNER 
    || '"".""' 
    || OBJECT_NAME 
    || '"";' || CHR(13)
FROM ALL_OBJECTS
WHERE OWNER = '{parameter.Schema}' AND OBJECT_TYPE = 'FUNCTION';
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get delete all custom type statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllCustomTypeScripts(SixnetDeleteAllCustomerTypeParameter parameter)
        {
            var sql = $@"
SELECT
    'DROP TYPE ""'
    || OWNER
    || '"".""'
    || OBJECT_NAME
    || '"";' || CHR(13)
FROM ALL_OBJECTS
WHERE OWNER = '{parameter.Schema}' AND OBJECT_TYPE = 'TYPE';
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get delete all procedure statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllProcedureScripts(SixnetDeleteAllProcedureParameter parameter)
        {
            var sql = $@"
  SELECT
    'DROP PROCEDURE ""'
    || OWNER
    || '"".""'
    || OBJECT_NAME
    || '"";' || CHR(13)
FROM ALL_OBJECTS
WHERE OWNER = '{parameter.Schema}' AND OBJECT_TYPE = 'PROCEDURE';
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #endregion

        #region Util

        #region Get combine operator

        /// <summary>
        /// Get combine operator
        /// </summary>
        /// <param name="combineType">Combine type</param>
        /// <returns>Return combine operator</returns>
        protected override string GetCombineOperator(SixnetCombineType combineType)
        {
            return combineType switch
            {
                SixnetCombineType.UnionAll => " UNION ALL ",
                SixnetCombineType.Union => " UNION ",
                SixnetCombineType.Except => " MINUS ",
                SixnetCombineType.Intersect => " INTERSECT ",
                _ => throw new InvalidOperationException($"{DatabaseType} not support {combineType}"),
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
        protected override string GetSqlDataType(SixnetDataField field, SixnetMigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            var dbTypeName = "";
            if (!string.IsNullOrWhiteSpace(field.DbType))
            {
                dbTypeName = field.DbType;
            }
            else
            {
                var dbType = field.GetDataType().GetDbType();
                var length = field.Length;
                var precision = field.Precision;
                var notFixedLength = options.NotFixedLength || field.HasDbFeature(SixnetFieldDbFeature.NotFixedLength);
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

        #region Get field identity

        /// <summary>
        /// Get field identity
        /// </summary>
        /// <param name="field">Field</param>
        /// <param name="options">Options</param>
        /// <returns></returns>
        protected override string GetFieldIdentity(SixnetDataField field, SixnetMigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            if (!field.InRole(SixnetFieldRole.Increment))
            {
                return string.Empty;
            }
            var startValue = field.StartValue;
            if (startValue == 0)
            {
                startValue = 1;
            }
            var incrementValue = field.IncrementValue;
            if (incrementValue == 0)
            {
                incrementValue = 1;
            }

            return $" GENERATED BY DEFAULT AS IDENTITY (START WITH {startValue} INCREMENT BY {incrementValue})";
        }

        #endregion

        #endregion
    }
}
