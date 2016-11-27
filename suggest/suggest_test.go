package suggest_test

import (
	"github.com/Symantec/scotty/suggest"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestEngine(t *testing.T) {

	Convey("Given a new engine", t, func() {
		engine := suggest.NewEngine()

		Convey("No suggestions given initially", func() {
			So(engine.Suggest(0, ""), ShouldBeEmpty)
		})

		Convey("After adding suggestions", func() {
			engine.Add("Hello")
			engine.Add("Suggest")
			engine.Add("A")
			engine.Add("Hi")
			engine.Add("A")
			engine.Add("Hi")
			engine.Add("Hello")
			engine.Add("Suggest")
			engine.Await()

			Convey("Suggestions should be in sorted order", func() {
				So(
					engine.Suggest(0, ""),
					ShouldResemble,
					[]string{"A", "Hello", "Hi", "Suggest"})
			})

			Convey("count = 0 gives all suggestions", func() {
				So(
					engine.Suggest(0, "H"),
					ShouldResemble,
					[]string{"Hello", "Hi"})
			})

			Convey("count > 0 gives at most count suggestions", func() {
				So(
					engine.Suggest(3, ""),
					ShouldResemble,
					[]string{"A", "Hello", "Hi"})
			})

			Convey("No match gives no suggestions", func() {
				So(engine.Suggest(0, "J"), ShouldBeEmpty)
			})

		})

	})
}

func TestStaticSuggest(t *testing.T) {

	Convey("Given static suggester", t, func() {
		suggestions := []string{
			"log", "logger", "loggest", "a", "an", "and", "aback"}
		engine := suggest.NewSuggester(suggestions...)

		Convey("Defensive copy of suggestions made", func() {
			suggestions[0] = "wrong"
			So(
				engine.Suggest(0, ""),
				ShouldResemble,
				[]string{
					"log", "logger", "loggest", "a", "an", "and", "aback"})
		})

		Convey("Suggestions in same order as original list", func() {
			So(
				engine.Suggest(0, "a"),
				ShouldResemble,
				[]string{
					"a", "an", "and", "aback"})
		})

		Convey("max suggestion count honored", func() {
			So(
				engine.Suggest(2, "log"),
				ShouldResemble,
				[]string{"log", "logger"})
		})

		Convey("no match gives no results", func() {
			So(engine.Suggest(2, "lol"), ShouldBeEmpty)
		})

	})
}
