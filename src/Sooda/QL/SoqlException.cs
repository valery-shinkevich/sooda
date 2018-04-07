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

namespace Sooda.QL
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    public class SoqlException : SoodaException
    {
        private int _p0;
        private int _p1;

        public SoqlException(string desc) : this(desc, -1, -1)
        {
        }

        public SoqlException(string desc, int p) : this(desc, p, p)
        {
        }

        public SoqlException(string desc, int p0, int p1)
            : base(desc)
        {
            _p0 = p0;
            _p1 = p1;
        }

        public SoqlException(string desc, Exception inner) : this(desc, -1, -1, inner)
        {
        }

        public SoqlException(string desc, int p, Exception inner) : this(desc, p, p, inner)
        {
        }

        public SoqlException(string desc, int p0, int p1, Exception inner)
            : base(desc, inner)
        {
            _p0 = p0;
            _p1 = p1;
        }

        protected SoqlException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public int StartPos
        {
            get { return _p0; }
        }

        public int EndPos
        {
            get { return _p1; }
        }
    }
}