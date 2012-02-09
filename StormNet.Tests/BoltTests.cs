using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Text;

namespace StormNet.Tests
{
    [TestFixture]
    public class BoltTests
    {
        [Test]
        public void ImplementsShellBoltProtocol()
        {
            // implements storm multilang protocol: https://github.com/nathanmarz/storm/wiki/Multilang-protocol

            string pidDir = Environment.CurrentDirectory;

            string stdin =
pidDir + @"
end
{""storm-config"": [] }
end
{""topology-context"": [] }
end
{""id"": -6955786537413359384, ""comp"": 1, ""stream"": 1, ""task"": 8, ""tuple"": [""squawk 1"", ""field2"", 3]}
end
outTasks
end
{""id"": -6955786537413359385, ""comp"": 2, ""stream"": 2, ""task"": 9, ""tuple"": [""squawk 2"", ""field2"", 4]}
end
outTasks
end
";

            string stdout;

            using (var stdinReader = new StringReader(stdin))
            {
                Console.SetIn(stdinReader);

                using (var stdoutWriter = new StringWriter())
                {
                    Console.SetOut(stdoutWriter);
                    var parrotBolt = new ParrotBolt();

                    var thread = new Thread(_ => parrotBolt.Run());
                    thread.Start();

                    // wait for both tuples to be processed
                    while (parrotBolt.NoOfTuplesProcessed < 2) { }

                    thread.Abort();
                    thread.Join();

                    stdout = stdoutWriter.ToString();
                }
            }

            string currentProcessId = Process.GetCurrentProcess().Id.ToString(CultureInfo.InvariantCulture);

            Assert.That(File.Exists(Path.Combine(@".\", currentProcessId)), 
                "A file should have been created in the specified directory with the Process Id as the name");

            using (var stdoutReader = new StringReader(stdout))
            {
                string pid = stdoutReader.ReadLine();
                Assert.That(pid, Is.EqualTo(currentProcessId), "The current process id should be returned on the stdout");

                CheckEmittedMessage(stdoutReader, new [] { "squawk 1", "field2", "3" });
                CheckEmittedMessage(stdoutReader, new [] { "squawk 2", "field2", "4" });
            }
        }

        private void CheckEmittedMessage(StringReader stdoutReader, string[] expectedTuple)
        {
            string tuple = stdoutReader.ReadLine();
            string end = stdoutReader.ReadLine();
            string sync = stdoutReader.ReadLine();

            Assert.That(end, Is.EqualTo("end"));
            Assert.That(sync, Is.EqualTo("sync"));

            var emittedMessage = JsonObject.Parse(tuple);
            Assert.That(emittedMessage.Get<string>("command"), Is.EqualTo("emit"), "The command type should be 'emit'");

            Assert.That(emittedMessage.Get<object[]>("tuple"), Is.EquivalentTo(expectedTuple));
        }
    }  

    public class ParrotBolt : Bolt
    {
        public int NoOfTuplesProcessed { get; set; }

        protected override void Process(StormTuple tuple)
        {
            Emit(tuple.Tuple);
            NoOfTuplesProcessed++;
        }
    }
}
