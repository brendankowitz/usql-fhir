using System;

namespace Microsoft.Health.Fhir.Usql.Json
{
    public class FhirPathValue
    {
        private FhirPathValue()
        {
        }

        public static FhirPathValue Scalar(string columnName, string fhirPath, Func<object, object> valueConverter = null)
        {
            if (columnName == null)
            {
                throw new ArgumentNullException(nameof(columnName));
            }

            if (fhirPath == null)
            {
                throw new ArgumentNullException(nameof(fhirPath));
            }

            return new FhirPathValue
            {
                ColumnName = columnName,
                FhirPath = fhirPath,
                ValueConverter = valueConverter
            };
        }

        public Func<object, object> ValueConverter { get; set; }

        public string FhirPath { get; set; }

        public string ColumnName { get; set; }
    }
}