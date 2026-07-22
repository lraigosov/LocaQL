//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chromedp/chromedp"
)

func jsString(v string) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// setValue assigns an input/textarea/select's value via the native property
// setter (so React-less vanilla-JS listeners still see it) and dispatches
// input+change so any live "input" listeners (e.g. explorer search) fire
// exactly like real typing would.
func setValue(id, value string) chromedp.Action {
	script := fmt.Sprintf(`(function(){
		const el = document.getElementById(%s);
		if (!el) throw new Error("setValue: missing #" + %s);
		const tag = el.tagName;
		const proto = tag === "TEXTAREA" ? window.HTMLTextAreaElement.prototype
			: tag === "SELECT" ? window.HTMLSelectElement.prototype
			: window.HTMLInputElement.prototype;
		const desc = Object.getOwnPropertyDescriptor(proto, "value");
		desc.set.call(el, %s);
		el.dispatchEvent(new Event("input", {bubbles: true}));
		el.dispatchEvent(new Event("change", {bubbles: true}));
	})()`, jsString(id), jsString(id), jsString(value))
	return chromedp.Evaluate(script, nil)
}

func setChecked(id string, checked bool) chromedp.Action {
	script := fmt.Sprintf(`(function(){
		const el = document.getElementById(%s);
		if (!el) throw new Error("setChecked: missing #" + %s);
		if (el.checked !== %v) el.click();
	})()`, jsString(id), jsString(id), checked)
	return chromedp.Evaluate(script, nil)
}

func clickID(id string) chromedp.Action {
	return chromedp.Click(`#`+id, chromedp.ByID)
}

// submitForm clicks the form's submit button rather than calling the DOM
// form.submit() method: the latter bypasses the "submit" event entirely
// (and thus the app's event.preventDefault()+fetch handler), triggering a
// real native form submission/navigation that tears down the page context.
func submitForm(id string) chromedp.Action {
	return chromedp.Click(`#`+id+` button[type="submit"]`, chromedp.ByQuery)
}

func textOf(id string, out *string) chromedp.Action {
	return chromedp.Text(`#`+id, out, chromedp.ByID)
}

func attrOf(id, attr string, out *string) chromedp.Action {
	return chromedp.AttributeValue(`#`+id, attr, out, new(bool), chromedp.ByID)
}

// dispatchKeydown fires a real "keydown" KeyboardEvent on the given element
// with the requested modifiers, matching how the app's own listeners read
// event.key/event.ctrlKey — the same technique testing-library's fireEvent
// uses for keyboard-shortcut assertions.
func dispatchKeydown(id, key string, ctrlOrMeta bool) chromedp.Action {
	script := fmt.Sprintf(`(function(){
		const el = document.getElementById(%s);
		if (!el) throw new Error("dispatchKeydown: missing #" + %s);
		el.focus();
		const ev = new KeyboardEvent("keydown", {
			key: %s,
			ctrlKey: %v,
			metaKey: %v,
			bubbles: true,
			cancelable: true,
		});
		el.dispatchEvent(ev);
	})()`, jsString(id), jsString(id), jsString(key), ctrlOrMeta, ctrlOrMeta)
	return chromedp.Evaluate(script, nil)
}

func evalString(script string, out *string) chromedp.Action {
	return chromedp.Evaluate(script, out)
}

func evalBool(script string, out *bool) chromedp.Action {
	return chromedp.Evaluate(script, out)
}

func waitEnabled(id string) chromedp.Action {
	return chromedp.WaitEnabled(`#`+id, chromedp.ByID)
}

func waitVisibleSel(sel string) chromedp.Action {
	return chromedp.WaitVisible(sel, chromedp.ByID)
}

func run(ctx context.Context, actions ...chromedp.Action) error {
	return chromedp.Run(ctx, actions...)
}

// pollTrue waits (up to 15s) for a JS boolean expression to become true,
// e.g. for the explorer tree to re-render after an async refreshAll().
func pollTrue(expr string) chromedp.Action {
	return chromedp.Poll(expr, nil,
		chromedp.WithPollingInterval(200*time.Millisecond),
		chromedp.WithPollingTimeout(30*time.Second),
	)
}

func treeContainsExpr(text string) string {
	return fmt.Sprintf(`document.getElementById("explorerTree").textContent.includes(%s)`, jsString(text))
}

// findSavedQueryLI returns the JS expression for the <li> whose label starts
// with the given saved-query name; saved query rows have no stable IDs.
func findSavedQueryLIExpr(name string) string {
	return fmt.Sprintf(`Array.from(document.querySelectorAll("#savedQueriesList li")).find(li => li.textContent.startsWith(%s))`, jsString(name))
}

func clickSavedQueryButton(name, buttonLabel string) chromedp.Action {
	script := fmt.Sprintf(`(function(){
		const li = %s;
		if (!li) throw new Error("saved query row not found: " + %s);
		const btn = Array.from(li.querySelectorAll("button")).find(b => b.textContent.trim() === %s);
		if (!btn) throw new Error("button not found: " + %s);
		btn.click();
	})()`, findSavedQueryLIExpr(name), jsString(name), jsString(buttonLabel), jsString(buttonLabel))
	return chromedp.Evaluate(script, nil)
}

func savedQueryLabelExpr(name string) string {
	return fmt.Sprintf(`(function(){
		const li = %s;
		return li ? li.querySelector("span").textContent : "";
	})()`, findSavedQueryLIExpr(name))
}
