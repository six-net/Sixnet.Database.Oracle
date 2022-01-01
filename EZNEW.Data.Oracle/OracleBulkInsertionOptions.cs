using System;
using System.Collections.Generic;
using System.Text;
using Oracle.ManagedDataAccess.Client;

namespace EZNEW.Data.Oracle
{
    /// <summary>
    /// Defines oracle bulk insertion options
    /// </summary>
    public class OracleBulkInsertionOptions : IBulkInsertionOptions
    {
        /// <summary>
        /// Indicates whether use transaction
        /// </summary>
        public bool UseTransaction { get; set; }

        /// <summary>
        /// Gets or sets the column mapping
        /// </summary>
        public List<OracleBulkCopyColumnMapping> ColumnMappings { get; }

        /// <summary>
        /// Gets or sets the number of rows to be processed before a notification event is generated
        /// </summary>
        public int NotifyAfter { get; set; }

        /// <summary>
        /// Gets or sets the number of seconds allowed for the bulk copy operation to complete before it is aborted
        /// </summary>
        public int BulkCopyTimeout { get; set; }

        /// <summary>
        /// Gets or sets the number of rows to be sent as a batch to the database
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// Indicates whether convert the table or field name to uppercase.
        /// Default is true.
        /// </summary>
        public bool Uppercase { get; set; } = true;

        /// <summary>
        /// Indicates whether wrap field and table name with quotes.
        ///  Default is true.
        /// </summary>
        public bool WrapWithQuotes { get; set; } = true;
    }
}
