using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hl7.Fhir.FhirPath;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Microsoft.Analytics.Types.Sql;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Usql.Json
{
    public static class FhirFunctions
    {
        private static readonly FhirJsonParser Parser = new FhirJsonParser();

        public static SqlMap<string, string> Tuple(string json, params FhirPathValue[] fhirPathScalarExpressions)
        {
            if (json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }

            if (fhirPathScalarExpressions == null)
            {
                throw new ArgumentNullException(nameof(fhirPathScalarExpressions));
            }

            return Tuple<string>(json, null, fhirPathScalarExpressions);
        }

        public static SqlMap<string, T> Tuple<T>(string json, params FhirPathValue[] fhirPathScalarExpressions)
        {
            if (json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }

            if (fhirPathScalarExpressions == null)
            {
                throw new ArgumentNullException(nameof(fhirPathScalarExpressions));
            }

            return Tuple<T>(json, null, fhirPathScalarExpressions);
        }

        public static SqlMap<string, string> Tuple(string json, string resourcePath, params FhirPathValue[] fhirPathScalarExpressions)
        {
            if (json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }

            if (fhirPathScalarExpressions == null)
            {
                throw new ArgumentNullException(nameof(fhirPathScalarExpressions));
            }

            return Tuple<string>(json, resourcePath, fhirPathScalarExpressions);
        }

        public static SqlMap<string, T> Tuple<T>(string json, string resourcePath, params FhirPathValue[] fhirPathScalarExpressions)
        {
            if (json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }

            if (fhirPathScalarExpressions == null)
            {
                throw new ArgumentNullException(nameof(fhirPathScalarExpressions));
            }

            if (string.IsNullOrEmpty(json) || fhirPathScalarExpressions == null || fhirPathScalarExpressions.Length <= 0)
            {
                return new SqlMap<string, T>();
            }

            string fhirJson = json;

            if (!string.IsNullOrEmpty(resourcePath))
            {
                var obj = JObject.Parse(json);
                var node = obj.SelectToken(resourcePath);
                fhirJson = node.ToString();
            }

            var root = Parser.Parse<Resource>(fhirJson);

            var results = new ConcurrentBag<KeyValuePair<string, T>>();

            Parallel.ForEach(fhirPathScalarExpressions, query =>
            {
                results.Add(new KeyValuePair<string, T>(query.ColumnName, ChangeType<T>(root.Scalar(query.FhirPath), query.ValueConverter)));
            });

            return SqlMap.Create(results);
        }

        internal static T ChangeType<T>(object val, Func<object, object> valueConverter)
        {
            if (valueConverter != null)
            {
                return (T)valueConverter(val);
            }

            try
            {
                return (T)val;
            }
            catch
            {
                if (typeof(T) == typeof(string))
                {
                    return (T)(object)val?.ToString();
                }

                // If this fails try to convert type
                return (T)Convert.ChangeType(val, typeof(T));
            }
        }
    }
}