#!/usr/bin/env python
# -*- coding: utf-8 -*-

class InvalidArgumentError(Exception):
	def __str__(self):
		return "invalid arguments"

class InvalidPathError(Exception):
	def __str__(self):
		return "path not valid"

class IsDirError(Exception):
	def __str__(self):
		return "path is a directory"

class IsFileError(Exception):
	def __str__(self):
		return "path is not directory"

class DirNotEmptyError(Exception):
	def __str__(self):
		return "directory not empty"

class SameFileError(Exception):
	def __str__(self):
		return "original path and target path are same"

class PathExistsError(Exception):
	def __str__(self):
		return "file or path is already exists"

class PathNotExistsError(Exception):
	def __str__(self):
		return "path does not exist"

class HdfsHostError(Exception):
	def __str__(self):
		return "operation category READ is not supported in state standby"

class ConnectionError(Exception):
	def __str__(self):
		return "HDFS connection aborted"
