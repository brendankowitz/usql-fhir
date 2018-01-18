using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Analytics.Interfaces;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Usql.Json
{
    [SqlUserDefinedExtractor(AtomicFileProcessing = true)]
    public class FhirJsonExtractor : IExtractor
    {
        private readonly string _rowPath;

        public FhirJsonExtractor(string rowPath = null)
        {
            _rowPath = rowPath;
        }

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            using (var reader = new JsonTextReader(new StreamReader(input.BaseStream)))
            {
                while (reader.Read())
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        var token = JObject.Load(reader);

                        if (!string.IsNullOrEmpty(_rowPath))
                        {
                            token = token.SelectTokens(_rowPath).OfType<JObject>().FirstOrDefault();
                        }

                        JObjectToRow(token, output);

                        yield return output.AsReadOnly();
                    }
                }
            }
        }

        protected virtual void JObjectToRow(JObject o, IUpdatableRow row)
        {
            foreach (var c in row.Schema)
            {
                object value = c.DefaultValue;

                if (o.TryGetValue(c.Name, out var token) && token != null)
                {
                    value = JsonFunctions.ConvertToken(token, c.Type) ?? c.DefaultValue;
                }

                row.Set(c.Name, value);
            }
        }
    }
}
