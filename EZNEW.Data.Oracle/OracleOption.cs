using System;
using System.Collections.Generic;
using System.Text;

namespace EZNEW.Data.Oracle
{
    /// <summary>
    /// Oracle option
    /// </summary>
    public class OracleOption
    {
        /// <summary>
        /// Whether wrap field and table name with quotes
        /// Default value is true
        /// </summary>
        public bool WrapWithQuotes { get; set; } = true;

        /// <summary>
        /// Whether converts field and table names to uppercase
        /// Default value is true
        /// </summary>
        public bool Uppercase { get; set; } = true;
    }
}
