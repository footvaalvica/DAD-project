﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TKVTransactionManager.Services
{
    /*
     * Lack of sufficient servers to establish a majority for a consensus
     */
    public class MajorityInsufficiencyException : Exception
    {
        public MajorityInsufficiencyException() : base("Insufficient processes to establish a majority.") { }
        public MajorityInsufficiencyException(string message) : base(message) { }
        public MajorityInsufficiencyException(string message, Exception innerException) : base(message, innerException) { }
    }
}