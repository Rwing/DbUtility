using Ivony.Data;
using Ivony.Data.Queries;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Ivony.Data
{
	public static class DataPithyExtensions
	{
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> TypeProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();

		private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName = new ConcurrentDictionary<RuntimeTypeHandle, string>();
		private static readonly ConcurrentDictionary<string, ISqlAdapter> AdapterDictionary = new ConcurrentDictionary<string, ISqlAdapter>();

		private static string GetTableName(Type type)
		{
			string name;
			if (!TypeTableName.TryGetValue(type.TypeHandle, out name))
			{
				name = type.Name;
				if (type.IsInterface && name.StartsWith("I"))
					name = name.Substring(1);

				TypeTableName[type.TypeHandle] = name;
			}
			return name;
		}

		private static IEnumerable<PropertyInfo> TypePropertiesCache(Type type)
		{
			IEnumerable<PropertyInfo> pis;
			if (TypeProperties.TryGetValue(type.TypeHandle, out pis))
				return pis;

			var properties = type.GetProperties().ToArray();
			TypeProperties[type.TypeHandle] = properties;
			return properties;
		}

		private static IEnumerable<PropertyInfo> KeyPropertiesCache(Type type)
		{

			IEnumerable<PropertyInfo> pis;
			if (KeyProperties.TryGetValue(type.TypeHandle, out pis))
				return pis;

			var allProperties = TypePropertiesCache(type);
			var keyProperties = new List<PropertyInfo>();
			var idProp = allProperties.Where(p => p.Name.ToLower().EndsWith("id")).FirstOrDefault();//第一个以id结尾的属性
			if (idProp != null)
			{
				keyProperties.Add(idProp);
			}

			KeyProperties[type.TypeHandle] = keyProperties;
			return keyProperties;
		}

		private static ISqlAdapter GetFormatter(IAsyncDbExecutor<ParameterizedQuery> executor)
		{
			string name = executor.GetType().Name;
			ISqlAdapter adapter;
			if (!AdapterDictionary.TryGetValue(name, out adapter))
			{
				var ns = typeof(ISqlAdapter).Namespace;
				var adapterName = ns + "." + name.Replace("Executor", "Adapter");
				adapter = (ISqlAdapter)Assembly.GetExecutingAssembly().CreateInstance(adapterName);
				if (adapter == null)
					throw new Exception(adapterName + " not found!");
				AdapterDictionary[name] = adapter;
			}
			return adapter;
		}

		public static int Add<T>(this IAsyncDbExecutor<ParameterizedQuery> executor, T entityToInsert) where T : class
		{
			var type = typeof(T);

			var tableName = GetTableName(type);
			var allProperties = TypePropertiesCache(type);
			var keyProperties = KeyPropertiesCache(type);
			var allPropertiesExceptKey = allProperties.Except(keyProperties);


			var adapter = GetFormatter(executor);
			var id = adapter.Insert(executor, tableName, allPropertiesExceptKey, keyProperties, entityToInsert);
			return id;
		}

		public static bool Update<T>(this IAsyncDbExecutor<ParameterizedQuery> executor, T entityToUpdate) where T : class
		{
			var type = typeof(T);

			var tableName = GetTableName(type);
			var allProperties = TypePropertiesCache(type);
			var keyProperties = KeyPropertiesCache(type);
			var allPropertiesExceptKey = allProperties.Except(keyProperties);

			if (!keyProperties.Any())
				throw new ArgumentException("Entity must have at least one [Key] property");

			var adapter = GetFormatter(executor);
			var isSuccess = adapter.Update(executor, tableName, allPropertiesExceptKey, keyProperties, entityToUpdate);
			return isSuccess;
		}
	}

	public interface ISqlAdapter
	{
		int Insert(IAsyncDbExecutor<ParameterizedQuery> executor, string tableName, IEnumerable<PropertyInfo> fieldProperties, IEnumerable<PropertyInfo> keyProperties, object entity);
		bool Update(IAsyncDbExecutor<ParameterizedQuery> executor, string tableName, IEnumerable<PropertyInfo> fieldProperties, IEnumerable<PropertyInfo> keyProperties, object entity);
	}

	public class SqlDbAdapter : ISqlAdapter
	{
		public int Insert(IAsyncDbExecutor<ParameterizedQuery> executor, string tableName, IEnumerable<PropertyInfo> fieldProperties, IEnumerable<PropertyInfo> keyProperties, object entity)
		{
			var template = "insert into {0} ({1}) values ({{...}}); select @@IDENTITY id";

			var fieldNames = string.Join(",", fieldProperties.Select(i => string.Format("[{0}]", i.Name)).ToArray());
			var values = fieldProperties.Select(i => i.GetValue(entity, null)).ToArray();

			var id = executor.T(string.Format(template, tableName, fieldNames), values).ExecuteScalar<int>();

			//NOTE: would prefer to use IDENT_CURRENT('tablename') or IDENT_SCOPE but these are not available on SQLCE
			if (keyProperties.Any())
				keyProperties.First().SetValue(entity, id, null);
			return id;
		}

		public bool Update(IAsyncDbExecutor<ParameterizedQuery> executor, string tableName, IEnumerable<PropertyInfo> fieldProperties, IEnumerable<PropertyInfo> keyProperties, object entity)
		{
			var template = "update {0} set {1} where {2}";
			int count = 0;

			var fieldString = string.Join(", ", fieldProperties.Select(i => string.Format("{0} = {{{1}}}", i.Name, count++)).ToArray());
			var whereString = string.Join(" and ", keyProperties.Select(i => string.Format("{0} = {{{1}}}", i.Name, count++)).ToArray());
			var fieldValues = fieldProperties.Select(i => i.GetValue(entity, null)).ToArray();
			var whereValues = keyProperties.Select(i => i.GetValue(entity, null)).ToArray();

			var total = executor.T(string.Format(template, tableName, fieldString, whereString), fieldValues.Concat(whereValues).ToArray()).ExecuteNonQuery();
			return total > 0;
		}
	}

	public class SQLiteAdapter : ISqlAdapter
	{

		public int Insert(IAsyncDbExecutor<ParameterizedQuery> executor, string tableName, IEnumerable<PropertyInfo> fieldProperties, IEnumerable<PropertyInfo> keyProperties, object entity)
		{
			throw new NotImplementedException();
		}

		public bool Update(IAsyncDbExecutor<ParameterizedQuery> executor, string tableName, IEnumerable<PropertyInfo> fieldProperties, IEnumerable<PropertyInfo> keyProperties, object entity)
		{
			throw new NotImplementedException();
		}
	}
}