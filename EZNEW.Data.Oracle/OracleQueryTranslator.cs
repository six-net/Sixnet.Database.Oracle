using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using EZNEW.Development.Command;
using EZNEW.Development.Query;
using EZNEW.Development.Query.Translation;
using EZNEW.Development.Entity;
using EZNEW.Exceptions;
using EZNEW.Data.Conversion;

namespace EZNEW.Data.Oracle
{
    /// <summary>
    /// Query translator implement for oracle
    /// </summary>
    internal class OracleQueryTranslator : IQueryTranslator
    {
        #region Fields

        const string EqualOperator = "=";
        const string GreaterThanOperator = ">";
        const string GreaterThanOrEqualOperator = ">=";
        const string NotEqualOperator = "<>";
        const string LessThanOperator = "<";
        const string LessThanOrEqualOperator = "<=";
        const string InOperator = "IN";
        const string NotInOperator = "NOT IN";
        const string LikeOperator = "LIKE";
        const string NotLikeOperator = "NOT LIKE";
        const string IsNullOperator = "IS NULL";
        const string NotNullOperator = "IS NOT NULL";
        const string DescKeyWord = "DESC";
        const string AscKeyWord = "ASC";
        public const string DefaultObjectPetName = "TB";
        const string TreeTableName = "RecurveTable";
        const string TreeTablePetName = "RTT";
        static readonly Dictionary<JoinType, string> joinOperatorDict = new Dictionary<JoinType, string>()
        {
            { JoinType.InnerJoin,"INNER JOIN" },
            { JoinType.CrossJoin,"CROSS JOIN" },
            { JoinType.LeftJoin,"LEFT JOIN" },
            { JoinType.RightJoin,"RIGHT JOIN" },
            { JoinType.FullJoin,"FULL JOIN" }
        };

        int subObjectSequence = 0;
        int recurveObjectSequence = 0;

        #endregion

        #region Properties

        /// <summary>
        /// Gets or sets the default object pet name
        /// </summary>
        public string ObjectPetName => DefaultObjectPetName;

        /// <summary>
        /// Gets or sets the parameter sequence
        /// </summary>
        public int ParameterSequence { get; set; } = 0;

        /// <summary>
        /// Gets or sets the data access context
        /// </summary>
        public DataAccessContext DataAccessContext { get; set; }

        /// <summary>
        /// Gets the database server
        /// </summary>
        DatabaseServer DatabaseServer => DataAccessContext.Server;

        /// <summary>
        /// Gets the database server type
        /// </summary>
        DatabaseServerType DatabaseServerType => OracleManager.CurrentDatabaseServerType;

        #endregion

        #region Functions

        /// <summary>
        /// Translate Query Object
        /// </summary>
        /// <param name="query">query object</param>
        /// <returns>translate result</returns>
        public QueryTranslationResult Translate(IQuery query)
        {
            Init();
            var result = ExecuteTranslation(query, QueryLocation.Top);
            return result;
        }

        /// <summary>
        /// Execute translation
        /// </summary>
        /// <param name="query">Query object</param>
        /// <param name="location">Query location</param>
        /// <param name="parameters">Parameters</param>
        /// <param name="objectName">Entity object name</param>
        /// <param name="useSort">Indicates whether use sort</param>
        /// <returns>Return a translation result</returns>
        public QueryTranslationResult ExecuteTranslation(IQuery query, QueryLocation location, CommandParameters parameters = null, string objectName = "", bool useSort = true)
        {
            if (query == null)
            {
                return QueryTranslationResult.Empty;
            }
            StringBuilder conditionBuilder = new StringBuilder();
            if (query.ExecutionMode == QueryExecutionMode.QueryObject)
            {
                StringBuilder sortBuilder = new StringBuilder();
                parameters = parameters ?? new CommandParameters();
                objectName = string.IsNullOrWhiteSpace(objectName) ? DefaultObjectPetName : objectName;
                var conditionObjectName = query.Recurve != null ? GetNewSubObjectPetName() : objectName;
                List<string> withScripts = new List<string>();
                string recurveTableName = string.Empty;
                string recurveTablePetName = string.Empty;

                #region condition

                if (!query.Conditions.IsNullOrEmpty())
                {
                    int index = 0;
                    foreach (var condition in query.Conditions)
                    {
                        var conditionResult = TranslateCondition(query, condition, parameters, conditionObjectName);
                        if (!conditionResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(conditionResult.WithScripts);
                            recurveTableName = conditionResult.RecurveObjectName;
                            recurveTablePetName = conditionResult.RecurvePetName;
                        }
                        conditionBuilder.Append($" {(index > 0 ? condition.ConnectionOperator.ToString() : string.Empty)} {conditionResult.ConditionString}");
                        index++;
                    }
                }

                #endregion

                #region sort

                if (useSort && !query.Sorts.IsNullOrEmpty())
                {
                    foreach (var sortEntry in query.Sorts)
                    {
                        sortBuilder.Append($"{ConvertSortFieldName(query, objectName, sortEntry)} {(sortEntry.Desc ? DescKeyWord : AscKeyWord)},");
                    }
                }

                #endregion

                #region combine

                StringBuilder combineBuilder = new StringBuilder();
                if (!query.Combines.IsNullOrEmpty())
                {
                    foreach (var combineEntry in query.Combines)
                    {
                        if (combineEntry?.Query == null)
                        {
                            continue;
                        }
                        var combineObjectPetName = GetNewSubObjectPetName();
                        string combineObjectName = DataAccessContext.GetCombineEntityObjectName(combineEntry.Query);
                        var combineQueryResult = ExecuteTranslation(combineEntry.Query, QueryLocation.Combine, parameters, combineObjectPetName, true);
                        string combineConditionString = string.IsNullOrWhiteSpace(combineQueryResult.ConditionString) ? string.Empty : $"WHERE {combineQueryResult.ConditionString}";
                        combineBuilder.Append($" {GetCombineOperator(combineEntry.Type)} SELECT {string.Join(",", OracleManager.FormatQueryFields(combineObjectPetName, query, query.GetEntityType(), true, false))} FROM {OracleManager.FormatTableName(combineObjectName)} {combineObjectPetName} {(combineQueryResult.AllowJoin ? combineQueryResult.JoinScript : string.Empty)} {combineConditionString}");
                        if (!combineQueryResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(combineQueryResult.WithScripts);
                            recurveTableName = combineQueryResult.RecurveObjectName;
                            recurveTablePetName = combineQueryResult.RecurvePetName;
                        }
                    }

                }

                #endregion

                #region join

                bool allowJoin = true;
                StringBuilder joinBuilder = new StringBuilder();
                if (!query.Joins.IsNullOrEmpty())
                {
                    foreach (var joinEntry in query.Joins)
                    {
                        var joinQueryEntityType = joinEntry?.JoinQuery?.GetEntityType();
                        if (joinQueryEntityType == null)
                        {
                            throw new EZNEWException("IQuery object must set entity type if use in join operation");
                        }
                        string joinObjName = GetNewSubObjectPetName();
                        var joinQueryResult = ExecuteTranslation(joinEntry.JoinQuery, QueryLocation.Join, parameters, joinObjName, true);
                        string joinQueryObjectName = DataAccessContext.GetJoinEntityObjectName(joinEntry.JoinQuery);
                        if (string.IsNullOrWhiteSpace(joinQueryResult.CombineScript))
                        {
                            var joinConnection = GetJoinCondition(query, joinEntry, conditionObjectName, joinObjName);
                            if (!string.IsNullOrWhiteSpace(joinQueryResult.ConditionString))
                            {
                                conditionBuilder.Append($"{(conditionBuilder.Length == 0 ? string.Empty : " AND ")}{joinQueryResult.ConditionString}");
                            }

                            joinBuilder.Append($" {GetJoinOperator(joinEntry.JoinType)} {OracleManager.FormatTableName(joinQueryObjectName)} {joinObjName}{joinConnection}");
                            if (joinQueryResult.AllowJoin && !string.IsNullOrWhiteSpace(joinQueryResult.JoinScript))
                            {
                                joinBuilder.Append($" {joinQueryResult.JoinScript}");
                            }
                        }
                        else
                        {
                            string combineJoinObjName = GetNewSubObjectPetName();
                            string joinConnection = GetJoinCondition(query, joinEntry, conditionObjectName, combineJoinObjName);
                            string combineQueryFields = string.Join(",", OracleManager.FormatQueryFields(joinObjName, joinEntry.JoinQuery, joinQueryEntityType, true, false));
                            string combineCondition = string.IsNullOrWhiteSpace(joinQueryResult.ConditionString) ? string.Empty : "WHERE " + joinQueryResult.ConditionString;
                            joinBuilder.Append($" {GetJoinOperator(joinEntry.JoinType)} (SELECT {combineQueryFields} FROM {OracleManager.FormatTableName(joinQueryObjectName)} {joinObjName} {(joinQueryResult.AllowJoin ? joinQueryResult.JoinScript : string.Empty)} {combineCondition} {joinQueryResult.CombineScript}) {combineJoinObjName}{joinConnection}");
                        }
                        if (!joinQueryResult.WithScripts.IsNullOrEmpty())
                        {
                            withScripts.AddRange(joinQueryResult.WithScripts);
                            recurveTableName = joinQueryResult.RecurveObjectName;
                            recurveTablePetName = joinQueryResult.RecurvePetName;
                        }
                    }
                }
                string joinScript = joinBuilder.ToString();

                #endregion

                #region recurve script

                string conditionString = conditionBuilder.ToString();
                if (query.Recurve != null)
                {
                    allowJoin = false;
                    string nowConditionString = conditionString;
                    EntityField recurveField = DataManager.GetField(DatabaseServerType, query, query.Recurve.DataField);
                    EntityField recurveRelationField = DataManager.GetField(DatabaseServerType, query, query.Recurve.RelationField);
                    DataAccessContext.SetActivityQuery(query, location);
                    string queryObjectName = recurveTableName = DataManager.GetEntityObjectName(DataAccessContext);
                    recurveTablePetName = conditionObjectName;
                    string recurveFieldName = OracleManager.FormatFieldName(recurveField.FieldName);
                    string recurveRelationFieldName = OracleManager.FormatFieldName(recurveRelationField.FieldName);
                    string withScript = $"SELECT {conditionObjectName}.{recurveFieldName} FROM {OracleManager.FormatTableName(queryObjectName)} {conditionObjectName} {joinScript} " +
                        $"START WITH {(string.IsNullOrWhiteSpace(nowConditionString) ? "1 = 1 " : nowConditionString)} " +
                        $"CONNECT BY PRIOR {(query.Recurve.Direction == RecurveDirection.Up ? $"{conditionObjectName}.{recurveRelationFieldName} = {conditionObjectName}.{recurveFieldName}" : $"{conditionObjectName}.{recurveFieldName} = {conditionObjectName}.{recurveRelationFieldName}")}";
                    conditionString = $"{objectName}.{recurveFieldName} IN ({withScript})";
                    withScripts.Add(withScript);
                }

                #endregion

                var result = QueryTranslationResult.Create(conditionString, sortBuilder.ToString().Trim(','), parameters);
                result.JoinScript = joinScript;
                result.AllowJoin = allowJoin;
                result.WithScripts = withScripts;
                result.RecurveObjectName = recurveTableName;
                result.RecurvePetName = recurveTablePetName;
                result.CombineScript = combineBuilder.ToString();
                return result;
            }
            else
            {
                conditionBuilder.Append(query.Text);
                return QueryTranslationResult.Create(conditionBuilder.ToString(), string.Empty, query.TextParameters);
            }
        }

        /// <summary>
        /// Translate condition
        /// </summary>
        /// <param name="sourceQuery">Source query</param>
        /// <param name="condition">Condition</param>
        /// <param name="parameters">Command parameters</param>
        /// <param name="objectName">Object name</param>
        /// <returns></returns>
        QueryTranslationResult TranslateCondition(IQuery sourceQuery, ICondition condition, CommandParameters parameters, string objectName)
        {
            if (condition == null)
            {
                return QueryTranslationResult.Empty;
            }
            if (condition is Criterion criterion)
            {
                return TranslateCriterion(sourceQuery, criterion, parameters, objectName);
            }
            //IQuery groupQuery = condition.Item2 as IQuery;
            if (condition is IQuery groupQuery && !groupQuery.Conditions.IsNullOrEmpty())
            {
                groupQuery.SetEntityType(sourceQuery.GetEntityType());
                var conditionCount = groupQuery.Conditions.GetCount();
                if (conditionCount == 1)
                {
                    var firstCondition = groupQuery.Conditions.First();
                    if (firstCondition is Criterion firstCriterion)
                    {
                        return TranslateCriterion(groupQuery, firstCriterion, parameters, objectName);
                    }
                    return TranslateCondition(groupQuery, firstCondition, parameters, objectName);
                }
                StringBuilder groupCondition = new StringBuilder("(");
                List<string> groupWithScripts = new List<string>();
                string recurveTableName = string.Empty;
                string recurveTablePetName = string.Empty;
                int index = 0;
                foreach (var groupQueryCondition in groupQuery.Conditions)
                {
                    var groupQueryConditionResult = TranslateCondition(groupQuery, groupQueryCondition, parameters, objectName);
                    if (!groupQueryConditionResult.WithScripts.IsNullOrEmpty())
                    {
                        recurveTableName = groupQueryConditionResult.RecurveObjectName;
                        recurveTablePetName = groupQueryConditionResult.RecurvePetName;
                        groupWithScripts.AddRange(groupQueryConditionResult.WithScripts);
                    }
                    groupCondition.Append($" {(index > 0 ? groupQueryCondition.ConnectionOperator.ToString() : string.Empty)} {groupQueryConditionResult.ConditionString}");
                    index++;
                }
                var groupResult = QueryTranslationResult.Create(groupCondition.Append(")").ToString());
                groupResult.RecurveObjectName = recurveTableName;
                groupResult.RecurvePetName = recurveTablePetName;
                groupResult.WithScripts = groupWithScripts;
                return groupResult;
            }
            return QueryTranslationResult.Empty;
        }

        /// <summary>
        /// Translate criterion
        /// </summary>
        /// <param name="sourceQuery">Source query</param>
        /// <param name="criterion">Criterion</param>
        /// <param name="parameters">Parameters</param>
        /// <param name="objectName">Object name</param>
        /// <returns>Return query translation result</returns>
        QueryTranslationResult TranslateCriterion(IQuery sourceQuery, Criterion criterion, CommandParameters parameters, string objectName)
        {
            if (criterion == null)
            {
                return QueryTranslationResult.Empty;
            }

            //constant
            if (criterion.IsBooleanConstant())
            {
                return QueryTranslationResult.Create(criterion.GetBooleanConstantCondition());
            }

            string sqlOperator = GetOperator(criterion.Operator);
            bool needParameter = OperatorNeedParameter(criterion.Operator);
            string criterionFieldName = ConvertCriterionFieldName(sourceQuery, objectName, criterion);
            if (!needParameter)
            {
                return QueryTranslationResult.Create($"{criterionFieldName} {sqlOperator}");
            }
            string parameterName = GetNewParameterName(criterion.Name);
            if (criterion.Value is IQuery subquery)
            {
                var subqueryObjectName = DataAccessContext.GetSubqueryEntityObjectName(subquery);
                if (subquery.QueryFields.IsNullOrEmpty())
                {
                    throw new EZNEWException($"Subquery: {subqueryObjectName} must set query field");
                }
                var subqueryField = DataManager.GetField(DatabaseServerType, subquery, subquery.QueryFields.First());
                string subqueryObjectPetName = GetNewSubObjectPetName();
                var subqueryLimitResult = GetSubqueryLimitCondition(sqlOperator, subquery.QuerySize);
                string limitString = subqueryLimitResult.Item2;
                bool userSort = !string.IsNullOrWhiteSpace(limitString);
                var subqueryTranslationResult = ExecuteTranslation(subquery, QueryLocation.Subuery, parameters, subqueryObjectPetName, userSort);
                string subquerySortString = subqueryTranslationResult.SortString;
                bool hasSort = !string.IsNullOrWhiteSpace(subquerySortString);
                subquerySortString = hasSort ? $"ORDER BY {subquerySortString}" : string.Empty;
                bool hasCombine = string.IsNullOrWhiteSpace(subqueryTranslationResult.CombineScript);
                string joinScript = subqueryTranslationResult.AllowJoin ? subqueryTranslationResult.JoinScript : string.Empty;
                limitString = userSort ? hasCombine || hasSort ? $"WHERE {limitString}" : limitString : string.Empty;
                string conditionString = OracleManager.CombineLimitCondition(subqueryTranslationResult.ConditionString, hasCombine || (userSort && hasSort) ? string.Empty : limitString);

                string subqueryCondition;
                string subqueryFieldName = OracleManager.FormatFieldName(subqueryField.FieldName);
                subqueryObjectName = OracleManager.FormatTableName(subqueryObjectName);
                if (subqueryLimitResult.Item1) //use wapper
                {
                    if (hasCombine)
                    {
                        subqueryCondition = userSort && hasSort
                            ? $"{criterionFieldName} {sqlOperator} (SELECT S{subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM {subqueryObjectName} {subqueryObjectPetName} {joinScript} {conditionString} {subqueryTranslationResult.CombineScript}) {subqueryObjectPetName} {subquerySortString}) {subqueryObjectPetName} {limitString}) S{subqueryObjectPetName})"
                            : $"{criterionFieldName} {sqlOperator} (SELECT S{subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM {subqueryObjectName} {subqueryObjectPetName} {joinScript} {conditionString} {subqueryTranslationResult.CombineScript}) {subqueryObjectPetName} {limitString} {subquerySortString}) S{subqueryObjectPetName})";
                    }
                    else
                    {
                        subqueryCondition = userSort && hasSort
                            ? $"{criterionFieldName} {sqlOperator} (SELECT S{subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM {subqueryObjectName} {subqueryObjectPetName} {joinScript} {conditionString} {subquerySortString}) {subqueryObjectPetName} {limitString}) S{subqueryObjectPetName})"
                            : $"{criterionFieldName} {sqlOperator} (SELECT S{subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM {subqueryObjectName} {subqueryObjectPetName} {joinScript} {conditionString} {subquerySortString}) S{subqueryObjectPetName})";
                    }
                }
                else
                {
                    if (hasCombine)
                    {
                        subqueryCondition = userSort && hasSort
                            ? $"{criterionFieldName} {sqlOperator} (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM {subqueryObjectName} {subqueryObjectPetName} {joinScript} {conditionString} {subqueryTranslationResult.CombineScript}) {subqueryObjectPetName} {subquerySortString}) {subqueryObjectPetName} {limitString})"
                            : $"{criterionFieldName} {sqlOperator} (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM {subqueryObjectName} {subqueryObjectPetName} {joinScript} {conditionString} {subqueryTranslationResult.CombineScript}) {subqueryObjectPetName} {limitString} {subquerySortString})";
                    }
                    else
                    {
                        subqueryCondition = userSort && hasSort
                            ? $"{criterionFieldName} {sqlOperator} (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM {subqueryObjectName} {subqueryObjectPetName} {joinScript} {conditionString} {subquerySortString}) {subqueryObjectPetName} {limitString})"
                            : $"{criterionFieldName} {sqlOperator} (SELECT {subqueryObjectPetName}.{subqueryFieldName} FROM {subqueryObjectName} {subqueryObjectPetName} {joinScript} {conditionString} {subquerySortString})";
                    }
                }
                var criterionResult = QueryTranslationResult.Create(subqueryCondition);
                if (!criterionResult.WithScripts.IsNullOrEmpty())
                {
                    criterionResult.WithScripts = new List<string>(subqueryTranslationResult.WithScripts);
                    criterionResult.RecurveObjectName = subqueryTranslationResult.RecurveObjectName;
                    criterionResult.RecurvePetName = subqueryTranslationResult.RecurvePetName;
                }
                return criterionResult;
            }
            var needWrapParameter = NeedWrapParameter(criterion);
            if (needWrapParameter)
            {
                var parameterValues = criterion.Value;
                List<string> parameterNames = new List<string>();
                int valParameterIndex = 0;
                foreach (var val in parameterValues)
                {
                    var valParameterName = $"{parameterName}{valParameterIndex++}";
                    parameterNames.Add($"{OracleManager.ParameterPrefix}{valParameterName}");
                    parameters.Add(valParameterName, FormatCriterionValue(criterion.Operator, val));
                }
                var criterionCondition = $"{criterionFieldName} {sqlOperator} ({string.Join(",", parameterNames)})";
                return QueryTranslationResult.Create(criterionCondition);
            }
            else
            {
                parameters.Add(parameterName, FormatCriterionValue(criterion.Operator, criterion.Value));
                var criterionCondition = $"{criterionFieldName} {sqlOperator} {OracleManager.ParameterPrefix}{parameterName}";
                return QueryTranslationResult.Create(criterionCondition);
            }
        }

        /// <summary>
        /// Get sql operator by criterion operator
        /// </summary>
        /// <param name="criterionOperator">Criterion operator</param>
        /// <returns></returns>
        string GetOperator(CriterionOperator criterionOperator)
        {
            string sqlOperator = string.Empty;
            switch (criterionOperator)
            {
                case CriterionOperator.Equal:
                    sqlOperator = EqualOperator;
                    break;
                case CriterionOperator.GreaterThan:
                    sqlOperator = GreaterThanOperator;
                    break;
                case CriterionOperator.GreaterThanOrEqual:
                    sqlOperator = GreaterThanOrEqualOperator;
                    break;
                case CriterionOperator.NotEqual:
                    sqlOperator = NotEqualOperator;
                    break;
                case CriterionOperator.LessThan:
                    sqlOperator = LessThanOperator;
                    break;
                case CriterionOperator.LessThanOrEqual:
                    sqlOperator = LessThanOrEqualOperator;
                    break;
                case CriterionOperator.In:
                    sqlOperator = InOperator;
                    break;
                case CriterionOperator.NotIn:
                    sqlOperator = NotInOperator;
                    break;
                case CriterionOperator.Like:
                case CriterionOperator.BeginLike:
                case CriterionOperator.EndLike:
                    sqlOperator = LikeOperator;
                    break;
                case CriterionOperator.NotLike:
                case CriterionOperator.NotBeginLike:
                case CriterionOperator.NotEndLike:
                    sqlOperator = NotLikeOperator;
                    break;
                case CriterionOperator.IsNull:
                    sqlOperator = IsNullOperator;
                    break;
                case CriterionOperator.NotNull:
                    sqlOperator = NotNullOperator;
                    break;
            }
            return sqlOperator;
        }

        /// <summary>
        /// Indicates operator whether need parameter
        /// </summary>
        /// <param name="criterionOperator">Criterion operator</param>
        /// <returns></returns>
        bool OperatorNeedParameter(CriterionOperator criterionOperator)
        {
            bool needParameter = true;
            switch (criterionOperator)
            {
                case CriterionOperator.NotNull:
                case CriterionOperator.IsNull:
                    needParameter = false;
                    break;
            }
            return needParameter;
        }

        /// <summary>
        /// Format criterion value
        /// </summary>
        /// <param name="criterionOperator">Criterion operator</param>
        /// <param name="value">Value</param>
        /// <returns>Return formated criterion value</returns>
        dynamic FormatCriterionValue(CriterionOperator criterionOperator, dynamic value)
        {
            dynamic realValue = value;
            switch (criterionOperator)
            {
                case CriterionOperator.Like:
                case CriterionOperator.NotLike:
                    realValue = $"%{value}%";
                    break;
                case CriterionOperator.BeginLike:
                case CriterionOperator.NotBeginLike:
                    realValue = $"{value}%";
                    break;
                case CriterionOperator.EndLike:
                case CriterionOperator.NotEndLike:
                    realValue = $"%{value}";
                    break;
            }
            return realValue;
        }

        /// <summary>
        /// Convert criterion field name
        /// </summary>
        /// <param name="query">Query object</param>
        /// <param name="objectName">Object name</param>
        /// <param name="criterion">Criterion</param>
        /// <returns></returns>
        string ConvertCriterionFieldName(IQuery query, string objectName, Criterion criterion)
        {
            return ConvertFieldName(query, objectName, criterion.Name, criterion.Options?.FieldConversionOptions);
        }

        /// <summary>
        /// Convert sort field name
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <param name="sortEntry">Sort entry</param>
        /// <returns></returns>
        string ConvertSortFieldName(IQuery query, string objectName, SortEntry sortEntry)
        {
            return ConvertFieldName(query, objectName, sortEntry.Name, sortEntry.Options?.FieldConversionOptions);
        }

        /// <summary>
        /// Convert field name
        /// </summary>
        /// <param name="objectName">Object name</param>
        /// <param name="fieldName">Field name</param>
        /// <param name="fieldConversionOptions">Field conversion options</param>
        /// <returns>Return new field name</returns>
        string ConvertFieldName(IQuery query, string objectName, string fieldName, FieldConversionOptions fieldConversionOptions)
        {
            var field = DataManager.GetField(DatabaseServerType, query, fieldName);
            fieldName = field.FieldName;
            if (fieldConversionOptions == null)
            {
                return $"{objectName}.{OracleManager.FormatFieldName(field.FieldName)}";
            }
            var fieldConversionResult = OracleManager.ConvertField(DatabaseServer, fieldConversionOptions, objectName, fieldName);
            if (string.IsNullOrWhiteSpace(fieldConversionResult?.NewFieldName))
            {
                throw new EZNEWException($"{DatabaseServerType}-{fieldConversionOptions.ConversionName}:new field name is null or empty.");
            }
            return fieldConversionResult.NewFieldName;
        }

        /// <summary>
        /// Get join operator
        /// </summary>
        /// <param name="joinType">Join type</param>
        /// <returns></returns>
        string GetJoinOperator(JoinType joinType)
        {
            return joinOperatorDict[joinType];
        }

        /// <summary>
        /// Get join condition
        /// </summary>
        /// <param name="sourceQuery">Source query</param>
        /// <param name="joinEntry">Join entry</param>
        /// <param name="sourceObjectPetName">Source object pet name</param>
        /// <param name="targetObjectPetName">Target object pet name</param>
        /// <returns>Return join condition</returns>
        string GetJoinCondition(IQuery sourceQuery, JoinEntry joinEntry, string sourceObjectPetName, string targetObjectPetName)
        {
            if (joinEntry.JoinType == JoinType.CrossJoin)
            {
                return string.Empty;
            }
            var joinFields = joinEntry?.JoinFields.Where(r => !string.IsNullOrWhiteSpace(r.Key) && !string.IsNullOrWhiteSpace(r.Value));
            var sourceEntityType = sourceQuery.GetEntityType();
            var targetEntityType = joinEntry.JoinQuery.GetEntityType();
            bool useValueAsSource = false;
            if (joinFields.IsNullOrEmpty())
            {
                if (sourceEntityType == targetEntityType)
                {
                    var primaryKeys = EntityManager.GetPrimaryKeys(sourceEntityType);
                    if (primaryKeys.IsNullOrEmpty())
                    {
                        return string.Empty;
                    }
                    joinFields = primaryKeys.ToDictionary(c => c, c => c);
                }
                else
                {
                    joinFields = EntityManager.GetRelationFields(sourceEntityType, targetEntityType);
                    if (joinFields.IsNullOrEmpty())
                    {
                        useValueAsSource = true;
                        joinFields = EntityManager.GetRelationFields(targetEntityType, sourceEntityType);
                    }
                }
                if (joinFields.IsNullOrEmpty())
                {
                    return string.Empty;
                }
            }
            List<string> joinList = new List<string>();
            foreach (var joinField in joinFields)
            {
                if (string.IsNullOrWhiteSpace(joinField.Key) || string.IsNullOrWhiteSpace(joinField.Value))
                {
                    continue;
                }
                var sourceField = DataManager.GetField(DatabaseServerType, sourceEntityType, joinField.Key);
                var targetField = DataManager.GetField(DatabaseServerType, targetEntityType, joinField.Value);
                string sourceFieldName = OracleManager.FormatFieldName(sourceField.FieldName);
                string targetFieldName = OracleManager.FormatFieldName(targetField.FieldName);
                joinList.Add($" {sourceObjectPetName}.{(useValueAsSource ? targetFieldName : sourceFieldName)}{GetJoinOperator(joinEntry.Operator)}{targetObjectPetName}.{(useValueAsSource ? sourceFieldName : targetFieldName)}");
            }
            return joinList.IsNullOrEmpty() ? string.Empty : " ON" + string.Join(" AND", joinList);
        }

        /// <summary>
        /// Get sql operator by join operator
        /// </summary>
        /// <param name="joinOperator">Join operator</param>
        /// <returns>Return a sql operator</returns>
        string GetJoinOperator(JoinOperator joinOperator)
        {
            string sqlOperator = string.Empty;
            switch (joinOperator)
            {
                case JoinOperator.Equal:
                    sqlOperator = EqualOperator;
                    break;
                case JoinOperator.GreaterThan:
                    sqlOperator = GreaterThanOperator;
                    break;
                case JoinOperator.GreaterThanOrEqual:
                    sqlOperator = GreaterThanOrEqualOperator;
                    break;
                case JoinOperator.NotEqual:
                    sqlOperator = NotEqualOperator;
                    break;
                case JoinOperator.LessThan:
                    sqlOperator = LessThanOperator;
                    break;
                case JoinOperator.LessThanOrEqual:
                    sqlOperator = LessThanOrEqualOperator;
                    break;
            }
            return sqlOperator;
        }

        /// <summary>
        /// Get new recurve table name
        /// Item1:petname,Item2:fullname
        /// </summary>
        /// <returns></returns>
        Tuple<string, string> GetNewRecurveTableName()
        {
            var recurveIndex = (recurveObjectSequence++).ToString();
            return new Tuple<string, string>
                (
                    $"{TreeTablePetName}{recurveIndex}",
                    $"{TreeTableName}{recurveIndex}"
                );
        }

        /// <summary>
        /// Get a new sub object pet name
        /// </summary>
        /// <returns></returns>
        string GetNewSubObjectPetName()
        {
            return $"TSB{subObjectSequence++}";
        }

        /// <summary>
        /// Gets a new parameter name
        /// </summary>
        /// <returns></returns>
        string GetNewParameterName(string originParameterName)
        {
            return $"{originParameterName}{ParameterSequence++}";
        }

        /// <summary>
        /// Init translator
        /// </summary>
        void Init()
        {
            recurveObjectSequence = subObjectSequence = 0;
        }

        /// <summary>
        /// Get subquery limit condition
        /// Item1:use wapper subquery condition
        /// Item2:limit string
        /// </summary>
        /// <param name="sqlOperator">Sql operator</param>
        /// <param name="querySize">Query size</param>
        Tuple<bool, string> GetSubqueryLimitCondition(string sqlOperator, int querySize)
        {
            var limitString = string.Empty;
            bool useWapper = false;
            switch (sqlOperator)
            {
                case InOperator:
                case NotInOperator:
                    if (querySize > 0)
                    {
                        limitString = $"ROWNUM <= {querySize}";
                        useWapper = true;
                    }
                    break;
                default:
                    limitString = $"ROWNUM <= 1";
                    break;
            }
            return new Tuple<bool, string>(useWapper, limitString);
        }

        /// <summary>
        /// Get combine operator
        /// </summary>
        /// <param name="combineType">Combine type</param>
        /// <returns>Return combine operator</returns>
        string GetCombineOperator(CombineType combineType)
        {
            switch (combineType)
            {
                case CombineType.UnionAll:
                default:
                    return "UNION ALL";
                case CombineType.Union:
                    return "UNION";
                case CombineType.Except:
                    return "MINUS";
                case CombineType.Intersect:
                    return "INTERSECT";
            }
        }

        /// <summary>
        /// Indicates whether allow wrap parameter
        /// </summary>
        /// <param name="criterion">Criterion</param>
        /// <returns></returns>
        bool NeedWrapParameter(Criterion criterion)
        {
            bool needWrap = criterion.Operator == CriterionOperator.In || criterion.Operator == CriterionOperator.NotIn;
            if (needWrap)
            {
                var value = criterion.Value;
                if (value is IEnumerable values)
                {
                    Type valueType = null;
                    foreach (var val in values)
                    {
                        valueType = val.GetType();
                        break;
                    }
                    needWrap = valueType == typeof(TimeSpan);
                }
                else
                {
                    needWrap = false;
                }

            }
            return needWrap;
        }

        #endregion
    }
}
