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

namespace Sooda
{
    using System.Collections.Generic;
    using System.Reflection;

    ///Reflection based implementation, with caching
    public abstract class SoodaObjectReflectionCachingFieldValues : SoodaObjectReflectionBasedFieldValues
    {
        private static readonly Dictionary<string, FieldInfo> FieldCache = new Dictionary<string, FieldInfo>();

        protected override FieldInfo GetField(string name)
        {
            var t = GetType();
            var key = t.FullName + "." + name;
            FieldInfo fi;
            if (FieldCache.TryGetValue(key, out fi))
            {
                return fi;
            }
            lock (FieldCache)
            {
                if (FieldCache.TryGetValue(key, out fi))
                {
                    return fi;
                }
                fi = t.GetField(name);
                if (fi != null)
                {
                    FieldCache[key] = fi;
                }
                return fi;
            }
        }

        protected SoodaObjectReflectionCachingFieldValues(string[] orderedFieldNames) : base(orderedFieldNames)
        {
        }

        protected SoodaObjectReflectionCachingFieldValues(SoodaObjectReflectionCachingFieldValues other) : base(other)
        {
        }
    }
}