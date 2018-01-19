// 
// Copyright (c) Microsoft and contributors.  All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// 
// See the License for the specific language governing permissions and
// limitations under the License.
// 

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Analytics.Interfaces;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Health.Fhir.Usql.Json
{
    /// <summary>
    /// Extracts FHIR Resources from JSON Format
    /// </summary>
    /// <remarks>
    /// Based on code: https://github.com/Azure/usql/blob/master/Examples/DataFormats/Microsoft.Analytics.Samples.Formats/Json/JsonExtractor.cs
    /// </remarks>
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
            if (input == null)
            {
                throw new ArgumentNullException(nameof(input));
            }

            if (output == null)
            {
                throw new ArgumentNullException(nameof(output));
            }

            using (var reader = new JsonTextReader(new StreamReader(input.BaseStream)))
            {
                while (reader.Read())
                {
                    if (reader.TokenType == JsonToken.StartObject)
                    {
                        var token = JObject.Load(reader);

                        if (!string.IsNullOrEmpty(_rowPath))
                        {
                            token = token.SelectToken(_rowPath) as JObject;
                        }

                        JObjectToRow(token, output);

                        yield return output.AsReadOnly();
                    }
                }
            }
        }

        protected virtual void JObjectToRow(JObject obj, IUpdatableRow row)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            if (row == null)
            {
                throw new ArgumentNullException(nameof(row));
            }

            foreach (var c in row.Schema)
            {
                object value = c.DefaultValue;

                // Attempt to fetch the property of an object from the name of the column
                if (obj.TryGetValue(c.Name, out var token) && token != null)
                {
                    value = ConvertToken(token, c.Type) ?? c.DefaultValue;
                }
                // If an '_' is in the column name, use this as a path into the JSON object structure
                else if (c.Name.Contains("_"))
                {
                    var path = c.Name.Replace("_", ".");
                    var selected = obj.SelectToken(path);
                    if (selected != null)
                    {
                        value = ConvertToken(selected, c.Type) ?? c.DefaultValue;
                    }
                }

                row.Set(c.Name, value);
            }
        }

        /// <remarks>
        /// Based on code: https://github.com/Azure/usql/blob/master/Examples/DataFormats/Microsoft.Analytics.Samples.Formats/Json/JsonExtractor.cs
        /// </remarks>
        internal static string GetTokenString(JToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Null:
                case JTokenType.Undefined:
                    return null;

                case JTokenType.String:
                    return (string)token;

                case JTokenType.Integer:
                case JTokenType.Float:
                case JTokenType.Boolean:
                    // For scalars we simply delegate to Json.Net (JsonConvert) for string conversions
                    //  This ensures the string conversion matches the JsonTextWriter
                    return JsonConvert.ToString(((JValue)token).Value);

                case JTokenType.Date:
                case JTokenType.TimeSpan:
                case JTokenType.Guid:
                    // For scalars we simply delegate to Json.Net (JsonConvert) for string conversions
                    //  Note: We want to leverage JsonConvert to ensure the string conversion matches the JsonTextWriter
                    //        However that places surrounding quotes for these data types.
                    var v = JsonConvert.ToString(((JValue)token).Value);
                    return v != null && v.Length > 2 && v[0] == '"' && v[v.Length - 1] == '"' ? v.Substring(1, v.Length - 2) : v;

                default:
                    // For containers we delegate to Json.Net (JToken.ToString/WriteTo) which is capable of serializing all data types, including nested containers
                    return token.ToString();
            }
        }

        /// <remarks>
        /// Based on code: https://github.com/Azure/usql/blob/master/Examples/DataFormats/Microsoft.Analytics.Samples.Formats/Json/JsonExtractor.cs
        /// </remarks>
        internal static object ConvertToken(JToken token, Type type)
        {
            try
            {
                if (type == typeof(string))
                {
                    return GetTokenString(token);
                }

                // We simply delegate to Json.Net for data conversions
                return token.ToObject(type);
            }
            catch (Exception e)
            {
                // Make this easier to debug (with field and type context)
                //  Note: We don't expose the actual value to be converted in the error message (since it might be sensitive, information disclosure)
                throw new JsonSerializationException(
                    string.Format(typeof(JsonToken).Namespace + " failed to deserialize '{0}' from '{1}' to '{2}'", token.Path, token.Type.ToString(), type.FullName),
                    e);
            }
        }
    }
}
