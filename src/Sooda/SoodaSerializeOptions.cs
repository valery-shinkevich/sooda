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
    using System;

    /// <summary>
    /// Specifies options for serializing transactions and objects to XML.
    /// </summary>
    [Flags]
    public enum SoodaSerializeOptions
    {
        /// <summary>
        /// Serialize dirty fields only.
        /// </summary>
        DirtyOnly = 0,

        /// <summary>
        /// Include non-dirty fields in serialization output. They will be marked as <c>dirty="false"</c>.
        /// </summary>
        IncludeNonDirtyFields = 0x1,

        /// <summary>
        /// Include non-dirty objects in serialization output. They will be marked as <c>dirty="false"</c>.
        /// </summary>
        IncludeNonDirtyObjects = 0x2,

        /// <summary>
        /// Include additional debug information in serialization output. It will be ignored during deserialization.
        /// </summary>
        IncludeDebugInfo = 0x4,

        Canonical = 0x8
    }
}