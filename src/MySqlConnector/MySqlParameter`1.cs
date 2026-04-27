using System.Buffers.Binary;
using System.Buffers.Text;
using System.Data;
using System.Globalization;
using System.Runtime.CompilerServices;
using MySqlConnector.Core;
using MySqlConnector.Protocol.Serialization;

namespace MySqlConnector;

/// <summary>
/// Strongly-typed counterpart to <see cref="MySqlParameter"/>. Stores the parameter value in a
/// typed slot to avoid boxing on assignment for value types, and pins <see cref="MySqlDbType"/>
/// at construction so the wire-time type-mapping inference is unnecessary.
/// </summary>
/// <remarks>
/// The <c>typeof(T) ==</c> tests in the overrides are JIT-time constants for closed generics:
/// each closed instantiation (e.g. <c>MySqlParameter&lt;int&gt;</c>) reduces to the matching
/// branch only, so there's no runtime type-test chain.
/// </remarks>
public sealed class MySqlParameter<T> : MySqlParameter
{
	private T m_value = default!;
	private bool m_hasValue;

	// Setting MySqlDbType also pins m_dbType and HasSetDbType, so the per-call inference
	// in MySqlParameter.Value's setter and SingleCommandPayloadCreator is bypassed.
	public MySqlParameter() =>
		MySqlDbType = GetMySqlDbType();

	public T TypedValue
	{
		get => m_value;
		set
		{
			m_value = value;
			m_hasValue = value is not null;
		}
	}

	public override object? Value
	{
		get => m_hasValue ? (object?) m_value : null;
		set
		{
			if (value is null || value == DBNull.Value)
			{
				m_value = default!;
				m_hasValue = false;
			}
			else if (value is T t)
			{
				m_value = t;
				m_hasValue = true;
			}
			else
			{
				throw new InvalidCastException(
					$"Cannot assign value of type '{value.GetType()}' to MySqlParameter<{typeof(T).Name}>.");
			}
		}
	}

	public override void SetNull()
	{
		m_value = default!;
		m_hasValue = false;
	}

	public new MySqlParameter Clone() => throw new NotSupportedException("MySqlParameter<T> can't be cloned.");

	internal override bool IsNullForBinary => !m_hasValue;
	internal override bool HasFixedTypeMapping => true;

	internal override void AppendBinary(ByteBufferWriter writer, StatementPreparerOptions options)
	{
		if (!m_hasValue)
			return;

		if (typeof(T) == typeof(bool))
		{
			writer.Write((byte) (Unsafe.As<T, bool>(ref m_value) ? 1 : 0));
		}
		else if (typeof(T) == typeof(sbyte))
		{
			writer.Write(unchecked((byte) Unsafe.As<T, sbyte>(ref m_value)));
		}
		else if (typeof(T) == typeof(byte))
		{
			writer.Write(Unsafe.As<T, byte>(ref m_value));
		}
		else if (typeof(T) == typeof(short))
		{
			writer.Write(unchecked((ushort) Unsafe.As<T, short>(ref m_value)));
		}
		else if (typeof(T) == typeof(ushort))
		{
			writer.Write(Unsafe.As<T, ushort>(ref m_value));
		}
		else if (typeof(T) == typeof(int))
		{
			writer.Write(Unsafe.As<T, int>(ref m_value));
		}
		else if (typeof(T) == typeof(uint))
		{
			writer.Write(Unsafe.As<T, uint>(ref m_value));
		}
		else if (typeof(T) == typeof(long))
		{
			writer.Write(unchecked((ulong) Unsafe.As<T, long>(ref m_value)));
		}
		else if (typeof(T) == typeof(ulong))
		{
			writer.Write(Unsafe.As<T, ulong>(ref m_value));
		}
		else if (typeof(T) == typeof(float))
		{
			Span<byte> bytes = stackalloc byte[4];
			BinaryPrimitives.WriteSingleLittleEndian(bytes, Unsafe.As<T, float>(ref m_value));
			writer.Write(bytes);
		}
		else if (typeof(T) == typeof(double))
		{
			Span<byte> bytes = stackalloc byte[8];
			BinaryPrimitives.WriteDoubleLittleEndian(bytes, Unsafe.As<T, double>(ref m_value));
			writer.Write(bytes);
		}
		else if (typeof(T) == typeof(decimal))
		{
			writer.WriteLengthEncodedAsciiString(
				Unsafe.As<T, decimal>(ref m_value).ToString(CultureInfo.InvariantCulture));
		}
		else
		{
			// Reference types (string, byte[], …) and any unsupported T: defer to the base, which
			// reads Value (overridden above to materialize directly from `m_value` — no boxing for
			// reference types) and dispatches via the existing `value is X` chain. Keeps reference-
			// type encoding bit-for-bit identical to upstream MSC.
			base.AppendBinary(writer, options);
		}
	}

	internal override void AppendSqlString(ByteBufferWriter writer, StatementPreparerOptions options)
	{
		if (!m_hasValue)
		{
			writer.Write("NULL"u8);
			return;
		}

		if (typeof(T) == typeof(bool))
		{
			writer.Write(Unsafe.As<T, bool>(ref m_value) ? "true"u8 : "false"u8);
		}
		else if (typeof(T) == typeof(sbyte))
		{
			Utf8Formatter.TryFormat(Unsafe.As<T, sbyte>(ref m_value), writer.GetSpan(4), out var bytesWritten);
			writer.Advance(bytesWritten);
		}
		else if (typeof(T) == typeof(byte))
		{
			Utf8Formatter.TryFormat(Unsafe.As<T, byte>(ref m_value), writer.GetSpan(3), out var bytesWritten);
			writer.Advance(bytesWritten);
		}
		else if (typeof(T) == typeof(short))
		{
			writer.WriteString(Unsafe.As<T, short>(ref m_value));
		}
		else if (typeof(T) == typeof(ushort))
		{
			writer.WriteString(Unsafe.As<T, ushort>(ref m_value));
		}
		else if (typeof(T) == typeof(int))
		{
			writer.WriteString(Unsafe.As<T, int>(ref m_value));
		}
		else if (typeof(T) == typeof(uint))
		{
			writer.WriteString(Unsafe.As<T, uint>(ref m_value));
		}
		else if (typeof(T) == typeof(long))
		{
			writer.WriteString(Unsafe.As<T, long>(ref m_value));
		}
		else if (typeof(T) == typeof(ulong))
		{
			writer.WriteString(Unsafe.As<T, ulong>(ref m_value));
		}
		else if (typeof(T) == typeof(float))
		{
			var value = Unsafe.As<T, float>(ref m_value);
#if NET8_0_OR_GREATER
			value.TryFormat(writer.GetSpan(14), out var bytesWritten, "R", CultureInfo.InvariantCulture);
			writer.Advance(bytesWritten);
#else
			writer.WriteAscii(value.ToString("R", CultureInfo.InvariantCulture));
#endif
		}
		else if (typeof(T) == typeof(double))
		{
			var value = Unsafe.As<T, double>(ref m_value);
#if NET8_0_OR_GREATER
			value.TryFormat(writer.GetSpan(24), out var bytesWritten, "R", CultureInfo.InvariantCulture);
			writer.Advance(bytesWritten);
#else
			writer.WriteAscii(value.ToString("R", CultureInfo.InvariantCulture));
#endif
		}
		else if (typeof(T) == typeof(decimal))
		{
			var value = Unsafe.As<T, decimal>(ref m_value);
#if NET8_0_OR_GREATER
			value.TryFormat(writer.GetSpan(31), out var bytesWritten, default, CultureInfo.InvariantCulture);
			writer.Advance(bytesWritten);
#else
			writer.WriteAscii(value.ToString(CultureInfo.InvariantCulture));
#endif
		}
		else
		{
			// String / byte[] / unsupported T: defer to the base, which reads Value (which we
			// override above to materialize from the typed slot) and uses the existing chain.
			base.AppendSqlString(writer, options);
		}
	}

	private static MySqlDbType GetMySqlDbType()
	{
		if (typeof(T) == typeof(bool)) return MySqlDbType.Bool;
		if (typeof(T) == typeof(sbyte)) return MySqlDbType.Byte;
		if (typeof(T) == typeof(byte)) return MySqlDbType.UByte;
		if (typeof(T) == typeof(short)) return MySqlDbType.Int16;
		if (typeof(T) == typeof(ushort)) return MySqlDbType.UInt16;
		if (typeof(T) == typeof(int)) return MySqlDbType.Int32;
		if (typeof(T) == typeof(uint)) return MySqlDbType.UInt32;
		if (typeof(T) == typeof(long)) return MySqlDbType.Int64;
		if (typeof(T) == typeof(ulong)) return MySqlDbType.UInt64;
		if (typeof(T) == typeof(float)) return MySqlDbType.Float;
		if (typeof(T) == typeof(double)) return MySqlDbType.Double;
		if (typeof(T) == typeof(decimal)) return MySqlDbType.NewDecimal;
		if (typeof(T) == typeof(string)) return MySqlDbType.VarChar;
		if (typeof(T) == typeof(byte[])) return MySqlDbType.Blob;
		throw new NotSupportedException($"MySqlParameter<{typeof(T).Name}> is not supported.");
	}
}

public static class MySqlParameterExtensions
{
	/// <summary>
	/// Sets the typed value of a parameter created as <see cref="MySqlParameter{T}"/>. The cast is
	/// safe by construction: prepared-statement parameters are created with the matching <c>T</c>.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Set<T>(this MySqlParameter parameter, T value)
		=> ((MySqlParameter<T>) parameter).TypedValue = value;
}
