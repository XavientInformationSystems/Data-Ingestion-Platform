package com.xavient.dataingest.storm.vo;

import java.io.Serializable;

public class Book implements Serializable {

	private static final long serialVersionUID = -4665667933953621500L;

	private String id;
	private String genre;
	private String author;
	private String title;
	private String price;
	private String publish_date;

	private String description;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getPrice() {
		return price;
	}

	public void setPrice(String price) {
		this.price = price;
	}

	public String getPublish_date() {
		return publish_date;
	}

	public void setPublish_date(String publish_date) {
		this.publish_date = publish_date;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String toString() {
		return "Book [id=" + id + ", genre=" + genre + ", author=" + author + ", title=" + title + ", price=" + price
				+ ", publish_date=" + publish_date + ", description=" + description + "]";
	}

}