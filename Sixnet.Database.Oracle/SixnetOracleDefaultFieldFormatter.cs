using System.Collections.Generic;
using System;
using Sixnet.Development.Data.Field.Formatting;
using Sixnet.Exceptions;

namespace Sixnet.Database.Oracle
{
    /// <summary>
    /// Default default field formatter for oracle
    /// </summary>
    public class SixnetOracleDefaultFieldFormatter : ISixnetFieldFormatter
    {
        public string Format(SixnetFormatFieldContext context)
        {
            var formatOption = context.FormatSetting;
            var formatedFieldName = context.FieldName;
            var parameterString = formatOption.Parameter?.ToString();
            formatedFieldName = formatOption.Name switch
            {
                SixnetFieldFormatterNames.TO_STRING => $"CAST({formatedFieldName} AS VARCHAR2({(string.IsNullOrWhiteSpace(parameterString) ? "4000" : parameterString)}))",
                SixnetFieldFormatterNames.DISTINCT => $"DISTINCT {formatedFieldName}",
                SixnetFieldFormatterNames.IS_NULL => $"{formatedFieldName} IS NULL",
                SixnetFieldFormatterNames.NOT_NULL => $"{formatedFieldName} IS NOT NULL",
                SixnetFieldFormatterNames.CHARLENGTH => $"LENGTH({formatedFieldName})",
                SixnetFieldFormatterNames.COUNT => $"COUNT({formatedFieldName})",
                SixnetFieldFormatterNames.SUM => $"SUM({formatedFieldName})",
                SixnetFieldFormatterNames.MAX => $"MAX({formatedFieldName})",
                SixnetFieldFormatterNames.MIN => $"MIN({formatedFieldName})",
                SixnetFieldFormatterNames.AVG => $"AVG({formatedFieldName})",
                SixnetFieldFormatterNames.JSON_VALUE => $"JSON_VALUE({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.JSON_OBJECT => $"JSON_QUERY({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.AND => $"BITAND({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.OR => $"({formatedFieldName} + {parameterString} - BITAND({formatedFieldName}, {parameterString}))",
                SixnetFieldFormatterNames.XOR => $"({formatedFieldName} + {parameterString} - 2*BITAND({formatedFieldName}, {parameterString}))",
                SixnetFieldFormatterNames.NOT => $"(BITNOT({formatedFieldName}))",
                SixnetFieldFormatterNames.ADD => $"({formatedFieldName}+{parameterString})",
                SixnetFieldFormatterNames.SUBTRACT => $"({formatedFieldName}-{parameterString})",
                SixnetFieldFormatterNames.MULTIPLY => $"({formatedFieldName}*{parameterString})",
                SixnetFieldFormatterNames.DIVIDE => $"({formatedFieldName}/{parameterString})",
                SixnetFieldFormatterNames.MODULO => $"MOD({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.STRING_CONCAT => $"({formatedFieldName} || {parameterString})",
                SixnetFieldFormatterNames.DATE_TIME_DATE => $"TRUNC({formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_YEAR => $"EXTRACT(YEAR FROM {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_MONTH => $"EXTRACT(MONTH FROM {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_DAY => $"EXTRACT(DAY FROM {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_HOUR => $"EXTRACT(HOUR FROM {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_MINUTE => $"EXTRACT(MINUTE FROM {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_SECOND => $"EXTRACT(SECOND FROM {formatedFieldName})",
                SixnetFieldFormatterNames.DATE_TIME_FORMAT_STRING => $"TO_CHAR({formatedFieldName}, '{parameterString}')",
                SixnetFieldFormatterNames.DATE_STRING => $"TO_CHAR({formatedFieldName}, 'YYYY-MM-DD')",
                SixnetFieldFormatterNames.US_DATE_STRING => $"TO_CHAR({formatedFieldName}, 'MM/DD/YYYY')",
                SixnetFieldFormatterNames.JAPAN_DATE_STRING => $"TO_CHAR({formatedFieldName}, 'YYYY/MM/DD')",
                SixnetFieldFormatterNames.TIME_SPAN_DAYS => $"({parameterString} - {formatedFieldName})",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_DAYS => $"(({parameterString} - {formatedFieldName})*24*60*60/86400.0)",
                SixnetFieldFormatterNames.TIME_SPAN_HOURS => $"(({parameterString} - {formatedFieldName})*24)",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_HOURS => $"(({parameterString} - {formatedFieldName})*24.0)",
                SixnetFieldFormatterNames.TIME_SPAN_MINUTES => $"(({parameterString} - {formatedFieldName})*24*60)",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_MINUTES => $"(({parameterString} - {formatedFieldName})*24*60.0)",
                SixnetFieldFormatterNames.TIME_SPAN_SECONDS => $"(({parameterString} - {formatedFieldName})*24*60*60)",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_SECONDS => $"(({parameterString} - {formatedFieldName})*24*60*60.0)",
                SixnetFieldFormatterNames.TIME_SPAN_MILLISECONDS => $"(({parameterString} - {formatedFieldName})*24*60*60*1000)",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_MILLISECONDS => $"(({parameterString} - {formatedFieldName})*24*60*60*1000.0)",
                SixnetFieldFormatterNames.TO_LOWER => $"LOWER({formatedFieldName})",
                SixnetFieldFormatterNames.TO_UPPER => $"UPPER({formatedFieldName})",
                SixnetFieldFormatterNames.SUB_STRING => Substring(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_REPLACE => ReplaceString(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.DATE_TIME_ADD_DAY => $"{formatedFieldName} + {parameterString}",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MONTH => $"ADD_MONTHS({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.DATE_TIME_ADD_YEAR => $"ADD_MONTHS({formatedFieldName}, {parameterString}*12)",
                SixnetFieldFormatterNames.DATE_TIME_ADD_HOUR => $"{formatedFieldName} + NUMTODSINTERVAL({parameterString}, 'HOUR')",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MINUTE => $"{formatedFieldName} + NUMTODSINTERVAL({parameterString}, 'MINUTE')",
                SixnetFieldFormatterNames.DATE_TIME_ADD_SECOND => $"{formatedFieldName} + NUMTODSINTERVAL({parameterString}, 'SECOND')",
                SixnetFieldFormatterNames.CONVERT_TO_INT => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_BOOLEAN => $"(CASE WHEN {formatedFieldName} IN (1,'Y','TRUE') THEN 1 ELSE 0 END)",
                SixnetFieldFormatterNames.CONVERT_TO_DATE_TIME => $"CAST({formatedFieldName} AS DATE)",
                SixnetFieldFormatterNames.CONVERT_TO_DECIMAL => $"CAST({formatedFieldName} AS NUMBER(20,4))",
                SixnetFieldFormatterNames.CONVERT_TO_DOUBLE => $"CAST({formatedFieldName} AS BINARY_DOUBLE)",
                SixnetFieldFormatterNames.MATH_ROUND => $"ROUND({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.MATH_ABS => $"ABS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_CEILING => $"CEIL({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_FLOOR => $"FLOOR({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_TRUNCATE => $"TRUNC({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_SIGN => $"SIGN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_POW => $"POWER({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.MATH_SQRT => $"SQRT({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_EXP => $"EXP({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_LOG => $"LOG({parameterString}, {formatedFieldName})",
                SixnetFieldFormatterNames.MATH_COS => $"COS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_SIN => $"SIN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_TAN => $"TAN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ACOS => $"ACOS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ASIN => $"ASIN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ATAN => $"ATAN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ATAN2 => $"ATAN2({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.STRING_INDEX_OF => StringIndexOf(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_LAST_INDEX_OF => StringLastIndexOf(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.EXISTS => $"EXISTS{formatedFieldName}",
                SixnetFieldFormatterNames.NOT_EXISTS => $"NOT EXISTS{formatedFieldName}",
                _ => throw new SixnetException($"{context.Server.DatabaseType} does not support field formatter: {formatOption.Name}"),
            };
            return formatedFieldName;
        }

        #region Substring
        string Substring(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                return $"SUBSTR({formatedFieldName}, {tupeParameter.Item1 + 1}, {tupeParameter.Item2})";
            }
            else
            {
                return $"SUBSTR({formatedFieldName}, {parameter + 1})";
            }
        }
        #endregion

        #region Replace
        string ReplaceString(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                return $"REPLACE({formatedFieldName}, '{tupeTwoParameter.Item1}', '{tupeTwoParameter.Item2}')";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }
        #endregion

        #region String
        string StringIndexOf(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = tupeTwoParameter.Item1;
                var startIndex = tupeTwoParameter.Item2;
                return $"(INSTR({formatedFieldName}, '{charValue}', {startIndex + 1}) - 1)";
            }
            else
            {
                return $"(INSTR({formatedFieldName}, '{parameter}') - 1)";
            }
        }

        string StringLastIndexOf(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = tupeTwoParameter.Item1;
                var startIndex = tupeTwoParameter.Item2;
                return $"(INSTR({formatedFieldName}, '{charValue}', -1, 1) - 1)";
            }
            else
            {
                return $"(INSTR({formatedFieldName}, '{parameter}', -1, 1) - 1)";
            }
        }
        #endregion
    }
}
