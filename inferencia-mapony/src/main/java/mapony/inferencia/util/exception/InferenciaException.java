package mapony.inferencia.util.exception;

import java.io.Serializable;

public class InferenciaException  extends Exception implements Serializable {

	private static final long serialVersionUID = -5592649562724728507L;

	private String message;

	public final String getMessage() {
		return message;
	}

	public final void setMessage(String message) {
		this.message = message;
	}

	public InferenciaException() {
		super("Excepción");
	}

	public InferenciaException(Exception e, String message) {
		super(e);
		setMessage(message);
	}
	
	public InferenciaException(Exception e, String message, boolean debug) {
		super(e);
		setMessage(message);
	}
}
