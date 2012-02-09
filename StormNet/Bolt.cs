using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace StormNet
{
    public abstract class Bolt
    {
        private bool running = true;

        public void Run()
        {
            string heartbeatDir = Protocol.ReadStringMessage();
            Protocol.SendProcessId(heartbeatDir);
            var env = Protocol.GetEnvironment();
            try
            {
                while (running)
                {
                    var tuple = Protocol.ReadTuple();
                    Process(tuple);
                    Protocol.Sync();
                }
            }
            catch (Exception ex)
            {
                Protocol.Log(ex.ToString());
            }
        }

        public void Stop()
        {
            running = false;
        }

        protected void Emit(params object[] values)
        {
            Protocol.Emit(values);
        }

        protected abstract void Process(StormTuple tuple);
    }
}
