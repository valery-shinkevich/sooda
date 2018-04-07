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
    using System.Collections;
    using QL;

    public interface ISoodaObjectList : IList
    {
        SoodaObject GetItem(int pos);

        int PagedCount { get; }
        ISoodaObjectList GetSnapshot();
        ISoodaObjectList SelectFirst(int n);
        ISoodaObjectList SelectLast(int n);
        ISoodaObjectList SelectRange(int from, int to);
        ISoodaObjectList Filter(SoodaObjectFilter filter);
        ISoodaObjectList Filter(SoqlBooleanExpression filterExpression);
        ISoodaObjectList Filter(SoodaWhereClause whereClause);
        ISoodaObjectList Sort(IComparer comparer);
        ISoodaObjectList Sort(string sortOrder);
        ISoodaObjectList Sort(SoqlExpression sortExpression);
        ISoodaObjectList Sort(SoqlExpression sortExpression, SortOrder sortOrder);
    }
}