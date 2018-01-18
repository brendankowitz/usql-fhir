namespace Microsoft.Health.Fhir.Usql.Json
{
    public class FhirPathColumn
    {
        public static FhirPathColumn Create(string columnName, string fhirPath)
        {
            return new FhirPathColumn
            {
                ColumnName = columnName,
                FhirPath = fhirPath
            };
        }

        public string FhirPath { get; set; }

        public string ColumnName { get; set; }
    }
}