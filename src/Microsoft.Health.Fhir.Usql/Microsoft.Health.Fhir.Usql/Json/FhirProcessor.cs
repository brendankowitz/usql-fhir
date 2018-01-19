using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Hl7.Fhir.FhirPath;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Microsoft.Analytics.Interfaces;

namespace Microsoft.Health.Fhir.Usql.Json
{
    [SqlUserDefinedProcessor]
    public class FhirProcessor : IProcessor
    {
        private static readonly FhirJsonParser Parser = new FhirJsonParser();
        private readonly string _resourceJson;
        private readonly FhirPathValue[] _columns;
        private static readonly ConcurrentDictionary<Type, Type> FailedConversions = new ConcurrentDictionary<Type, Type>();

        public FhirProcessor(string resourceJson, params FhirPathValue[] columns)
        {
            _resourceJson = resourceJson ?? throw new ArgumentNullException(nameof(resourceJson));
            _columns = columns ?? throw new ArgumentNullException(nameof(columns));
        }

        public override IRow Process(IRow input, IUpdatableRow output)
        {
            if (input == null)
            {
                throw new ArgumentNullException(nameof(input));
            }

            if (output == null)
            {
                throw new ArgumentNullException(nameof(output));
            }

            string fhirJson = input.Get<string>(_resourceJson);
            var schema = output.Schema.ToArray();
            var root = Parser.Parse<Resource>(fhirJson);

            var results = new ConcurrentBag<Tuple<string, object>>();

            Parallel.ForEach(_columns, query =>
            {
                var column = schema.FirstOrDefault(x => string.Equals(query.ColumnName, x.Name, StringComparison.InvariantCultureIgnoreCase));
                results.Add(Tuple.Create(column?.Name ?? query.ColumnName, ChangeType(root.Scalar(query.FhirPath), column, query.ValueConverter)));
            });

            foreach (var item in results)
            {
                output.Set(item.Item1, item.Item2);
            }

            return output.AsReadOnly();
        }

        private object ChangeType(object val, IColumn schema, Func<object, object> valueConverter)
        {
            if (valueConverter != null)
            {
                return valueConverter(val);
            }

            if (schema != null && val != null)
            {
                // Prevent trying to cast to a type that throws an exception
                if (!(FailedConversions.TryGetValue(val.GetType(), out Type toType) && toType == schema.Type))
                {
                    try
                    {
                        return Convert.ChangeType(val, schema.Type);
                    }
                    catch
                    {
                        FailedConversions.TryAdd(val.GetType(), schema.Type);
                    }
                }

                return Convert.ChangeType(val.ToString(), schema.Type);
            }

            return val?.ToString();
        }
    }
}