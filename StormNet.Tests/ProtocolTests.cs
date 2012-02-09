using System;
using System.IO;
using NUnit.Framework;
using ServiceStack.Text;

namespace StormNet.Tests
{
    [TestFixture]
    public class ProtocolTests
    {
        [Test]
        public void EmitTuple()
        {
            using (var sw = new StringWriter())
            {
                Console.SetOut(sw); // redirect stdout to my writer

                using (var sr = new StringReader("end" + Environment.NewLine))
                {
                    Console.SetIn(sr);

                    Protocol.Emit(new object[] { "x" });

                    var emittedMessage = sw.ToString();
                    var json = JsonObject.Parse(emittedMessage);

                    Assert.That(json["command"], Is.EqualTo("emit"), "The command name should be 'emit'");
                    Assert.That(json["anchors"], Is.EqualTo("[]"), "No anchors were specified, the list should be empty");
                    Assert.That(json.Get<string[]>("tuple"), Is.EquivalentTo(new [] { "x" }));
                }
            }
        }
    }
}
