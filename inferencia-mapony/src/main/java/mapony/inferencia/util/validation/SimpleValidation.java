package mapony.inferencia.util.validation;

import org.apache.hadoop.io.Text;

import mapony.inferencia.util.cte.InferenciaCte;

public class SimpleValidation {

	/**
	 * Compares the two param String, trimming them, and returns true if they've the same value
	 * 
	 * @param expected
	 * @param actual
	 * @return True if expected has the same value as actual
	 */
	public static boolean isTrimExpectedEqualsTrimActual(String expected, String actual) {
		return expected.trim().compareTo(actual.trim()) == 0;
	}

	/**
	 * Compares the two param String, trimming them, and returns true if they don't have the same value
	 * 
	 * @param expected
	 * @param actual
	 * @return True if expected has not the same value as actual
	 */
	public static boolean isNotTrimExpectedEqualsTrimActual(String expected, String actual) {
		return expected.trim().compareTo(actual.trim()) != 0;
	}

	/**
	 * Compares the expected param with null and returns true if expected is not null
	 * 
	 * @param expected
	 * @return True if expected is not null
	 */
	public static boolean isExpectedNotNull(String expected) {
		return expected != null;
	}

	public static boolean isObjectNotNull(Object o) {
		return o != null;
	}

	public static boolean isObjectNull(Object o) {
		return o == null;
	}

	public static boolean isTextValueEmpty(Text expected) {
		String value = expected.toString();
		return isTrimExpectedEqualsEmpty(value);
	}

	/**
	 * Compares the expected param with null and returns true if expected is null
	 * 
	 * @param expected
	 * @return True if expected is null
	 */
	public static boolean isStringExpectedNull(String expected) {
		return expected == null;
	}

	/**
	 * Compares the param String with an empty String and returns true if they've the same value
	 * 
	 * @param expected
	 * @return true if expected equals ''
	 */
	public static boolean isTrimExpectedEqualsEmpty(String expected) {
		return InferenciaCte.EMPTY_STRING.compareTo(expected.trim()) == 0;
	}

}
