using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack.Text;

namespace StormNet
{
    public class Protocol
    {
        private static string ReadStringMessage()
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

        private static JsonObject ReadMessage()
        {
            string message = ReadStringMessage();
            return JsonObject.Parse(message);
        }

        private static void SendToParent(string s)
        {
            Console.Out.WriteLine(s);
            Console.Out.WriteLine("end");
            Console.Out.Flush();
        }

        private static void SendMessageToParent(object json)
        {
            SendToParent(json.ToJson());
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

        public static JsonObject Emit(object[] tuple, int? streamId = null, IEnumerable<StormTuple> anchors = null)
        {
            EmitTuple(tuple, streamId, anchors);
            return ReadMessage();
        }
    }
}
