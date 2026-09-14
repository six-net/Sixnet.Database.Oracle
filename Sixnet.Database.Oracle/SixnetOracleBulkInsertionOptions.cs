using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using Sixnet.Development.Data;
using Sixnet.Development.Data.Database;

namespace Sixnet.Database.Oracle
{
    /// <summary>
    /// Defines oracle bulk insertion options
    /// </summary>
    public class SixnetOracleBulkInsertionOptions : ISixnetBulkInsertionOptions
    {
        /// <summary>
        /// Gets or sets the data operation options
        /// </summary>
        public SixnetDataOperationOptions DataOperationOptions { get; set; }

        /// <summary>
        /// Gets or sets the bulk copy options
        /// </summary>
        public OracleBulkCopyOptions BulkCopyOptions { get; set; } = OracleBulkCopyOptions.Default;

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
    }
}
