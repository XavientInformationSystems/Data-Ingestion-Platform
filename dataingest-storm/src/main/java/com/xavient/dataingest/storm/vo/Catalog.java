package com.xavient.dataingest.storm.vo;

import java.io.Serializable;
import java.util.Arrays;

public class Catalog implements Serializable {

	private static final long serialVersionUID = 8898369117562558641L;

	private Book[] book;

	public Book[] getBook() {
		return book;
	}

	public void setBook(Book[] book) {
		this.book = book;
	}

	@Override
	public String toString() {
		return "Catalog [book=" + Arrays.toString(book) + "]";
	}

}