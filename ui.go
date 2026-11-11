package main

import _ "embed"

//go:embed dashboard.html
var dashboardHTMLContent string

func dashboardHTML() string {
	return dashboardHTMLContent
}
