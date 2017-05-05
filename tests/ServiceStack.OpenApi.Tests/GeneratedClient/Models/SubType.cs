// Code generated by Microsoft (R) AutoRest Code Generator 1.0.1.0
// Changes may cause incorrect behavior and will be lost if the code is
// regenerated.

namespace AutorestClient.Models
{
    using Newtonsoft.Json;
    using System.Linq;

    /// <summary>
    /// SubType
    /// </summary>
    /// <remarks>
    /// SubType
    /// </remarks>
    public partial class SubType
    {
        /// <summary>
        /// Initializes a new instance of the SubType class.
        /// </summary>
        public SubType()
        {
          CustomInit();
        }

        /// <summary>
        /// Initializes a new instance of the SubType class.
        /// </summary>
        public SubType(int id = default(int), string name = default(string))
        {
            Id = id;
            Name = name;
            CustomInit();
        }

        /// <summary>
        /// An initialization method that performs custom operations like setting defaults
        /// </summary>
        partial void CustomInit();

        /// <summary>
        /// </summary>
        [JsonProperty(PropertyName = "Id")]
        public int Id { get; set; }

        /// <summary>
        /// </summary>
        [JsonProperty(PropertyName = "Name")]
        public string Name { get; set; }

    }
}
