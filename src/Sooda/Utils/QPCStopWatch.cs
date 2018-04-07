//
// Copyright (c) 2003-2006 Jaroslaw Kowalski <jaak@jkowalski.net>
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// * Redistributions of source code must retain the above copyright notice,
//   this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//

namespace Sooda.Utils
{
    using System.Runtime.InteropServices;

    public class QPCStopWatch : StopWatch
    {
        private long _startTime;
        private long _stopTime;
        private static long _overhead;
        private static long _frequency;

        static QPCStopWatch()
        {
            QueryPerformanceFrequency(out _frequency);
            StopWatch callibration = new QPCStopWatch();
            long totalOverhead = 0;
            int loopCount = 0;
            for (int i = 0; i < 10; ++i)
            {
                callibration.Start();
                callibration.Stop();
            }
            for (int i = 0; i < 10; ++i)
            {
                callibration.Start();
                callibration.Stop();
                totalOverhead += ((QPCStopWatch) callibration).Ticks;
                loopCount++;
            }
            _overhead = totalOverhead/loopCount;
        }

        public override void Start()
        {
            QueryPerformanceCounter(out _startTime);
        }

        public override void Stop()
        {
            QueryPerformanceCounter(out _stopTime);
        }

        public long Ticks
        {
            get { return _stopTime - _startTime - _overhead; }
        }

        public override double Seconds
        {
            get { return (double) (_stopTime - _startTime - _overhead)/_frequency; }
        }

        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceCounter(out long val);

        [DllImport("kernel32.dll")]
        private static extern bool QueryPerformanceFrequency(out long val);
    }
}