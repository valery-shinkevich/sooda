//
// Copyright (c) 2003-2006 Jaroslaw Kowalski <jaak@jkowalski.net>
// Copyright (c) 2006-2014 Piotr Fusik <piotr@fusik.info>
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

using Sooda.Caching;
using Sooda.UnitTests.Objects;
using System;

namespace Sooda.UnitTests.TestCases
{
    public class TestSqlDataSource : Sooda.Sql.SqlDataSource, IDisposable
    {
        public TestSqlDataSource(string name) : base(_DatabaseSchema.GetSchema().GetDataSourceInfo(name)) { }

        public override void Close()
        {
            Console.WriteLine("TestSqlDataSource.Close({0})", Name);
        }

        public override void Commit()
        {
            Console.WriteLine("TestSqlDataSource.Commit()");
            // do nothing
        }

        public override void Rollback()
        {
            throw new NotSupportedException("Rollback not supported here!");
            // do nothing
        }

        public override void Open()
        {
            Console.WriteLine("Opening: {0}", this.Name);
            base.Open();
        }


        public new void Dispose()
        {
            Console.WriteLine("TestSqlDataSource.Dispose!");
            base.Close();
            SoodaCache.DefaultCache.Clear();;
        }
    }
}
