using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using ServiceStack.Text;

namespace StormNet
{
    static class Protocol
    {
        internal static string ReadStringMessage()
        {
            string msg = String.Empty;
            while (true)
            {
                string line = Console.ReadLine();
                if (line == "end")
                    break;

                msg += line + "\n";
            }
            return msg.TrimEnd('\n');
        }

        static JsonObject ReadMessage()
        {
            string message = ReadStringMessage();
            return JsonObject.Parse(message);
        }

        internal static StormTuple ReadTuple()
        {
            var json = ReadMessage();

            return new StormTuple
            {
                Id = json.JsonTo<long>("id"),
                Component = json.JsonTo<string>("comp"),
                Stream = json.JsonTo<string>("stream"),
                Task = json.JsonTo<string>("task"),
                Tuple = json.Get<string[]>("tuple")
            };
        }

        internal static void SendProcessId(string heartbeatDir)
        {
            int pid = Process.GetCurrentProcess().Id;
            var pidFile = Path.Combine(heartbeatDir, pid.ToString(CultureInfo.InvariantCulture));
            File.Create(pidFile);
            Console.WriteLine(pid);
        }

        internal static StormEnvironment GetEnvironment()
        {
            return new StormEnvironment
            {
                Config = ReadStringMessage(),
                Context = ReadStringMessage()
            };
        }

        private static void SendMessageToParent(object json)
        {
            SendToParent(json.ToJson());
        }

        private static void SendToParent(string s)
        {
            Console.Out.WriteLine(s);
            Console.Out.WriteLine("end");
            Console.Out.Flush();
        }

        private static void EmitTuple(object[] tuple, int? streamId = null, IEnumerable<StormTuple> anchors = null, long? direct = null)
        {
            long[] anchorIds = anchors != null ? anchors.Select(a => a.Id).ToArray() : new long[] { };

            var message = new Dictionary<string, object>
            {
                {"command", "emit"},
                {"anchors", anchorIds},
                {"tuple", tuple}
            };

            if (streamId != null) message.Add("stream", streamId);
            if (direct != null) message.Add("task", direct);

            SendMessageToParent(message);
        }

        internal static JsonObject Emit(object[] tuple, int? streamId = null, IEnumerable<StormTuple> anchors = null)
        {
            EmitTuple(tuple, streamId, anchors);
            return ReadMessage();
        }

        internal static void EmitDirect(int taskId, object[] tuple, int? streamId = null, IEnumerable<StormTuple> anchors = null)
        {
            EmitTuple(tuple, streamId, anchors, taskId);
        }

        internal static void Ack(StormTuple tuple)
        {
            SendMessageToParent(new Dictionary<string, object>
            {
                {"command", "ack"},
                {"id", tuple.Id}
            });
        }

        internal static void Fail(StormTuple tuple)
        {
            SendMessageToParent(new Dictionary<string, object>
            {
                {"command", "fail"},
                {"id", tuple.Id}
            });
        }

        internal static void Log(string message)
        {
            SendMessageToParent(new Dictionary<string, string>
            {
                { "command", "log" },
                { "msg", message }
            });
        }

        internal static void Sync()
        {
            Console.WriteLine("sync");
        }
    }
}
