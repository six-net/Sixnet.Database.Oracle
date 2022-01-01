using System;
using System.Collections.Generic;
using System.Text;
using EZNEW.Data.Conversion;
using EZNEW.Exceptions;

namespace EZNEW.Data.Oracle
{
    /// <summary>
    /// Default default field converter for oracle
    /// </summary>
    public class OracleDefaultFieldConverter : IFieldConverter
    {
        public FieldConversionResult Convert(FieldConversionContext fieldConversionContext)
        {
            if (string.IsNullOrWhiteSpace(fieldConversionContext?.ConversionName))
            {
                return null;
            }
            string formatedFieldName;
            switch (fieldConversionContext.ConversionName)
            {
                case FieldConversionNames.StringLength:
                    formatedFieldName = string.IsNullOrWhiteSpace(fieldConversionContext.ObjectName)
                        ? $"LENGTH({fieldConversionContext.ObjectName}.{OracleManager.FormatFieldName(fieldConversionContext.FieldName)})"
                        : $"LENGTH({OracleManager.FormatFieldName(fieldConversionContext.FieldName)})";
                    break;
                default:
                    throw new EZNEWException($"{OracleManager.CurrentDatabaseServerType} does not support field conversion: {fieldConversionContext.ConversionName}");
            }
            return new FieldConversionResult()
            {
                NewFieldName = formatedFieldName
            };
        }
    }
}
