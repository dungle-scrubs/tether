import ts from "typescript";

/** Visits syntax without treating comments or strings as identifier references. */
export function visit(node: ts.Node, check: (node: ts.Node) => void): void {
  check(node);
  ts.forEachChild(node, (child) => visit(child, check));
}

/** Identifies enclosing declarations and literal test titles without unstable line numbers. */
export function findingScope(node: ts.Node, source: ts.SourceFile): string {
  const owners: string[] = [];
  let current: ts.Node | undefined = node;
  while (current && current !== source) {
    if ("name" in current && current.name && ts.isIdentifier(current.name as ts.Node)) {
      owners.unshift(`${ts.SyntaxKind[current.kind]}:${(current.name as ts.Identifier).text}`);
    } else if (
      ts.isCallExpression(current) &&
      current.arguments[0] &&
      (ts.isStringLiteral(current.arguments[0]) ||
        ts.isNoSubstitutionTemplateLiteral(current.arguments[0]))
    ) {
      owners.unshift(`call:${current.arguments[0].text}`);
    }
    current = current.parent;
  }
  return owners.join("/");
}

/** Recognizes named functions, class properties and contract property or method members. */
function documentationTarget(node: ts.Node): ts.Node | null {
  if (
    (ts.isFunctionDeclaration(node) ||
      ts.isMethodDeclaration(node) ||
      ts.isConstructorDeclaration(node) ||
      ts.isGetAccessorDeclaration(node) ||
      ts.isSetAccessorDeclaration(node)) &&
    node.body
  )
    return node;
  if (
    ts.isVariableDeclaration(node) &&
    node.initializer &&
    (ts.isArrowFunction(node.initializer) || ts.isFunctionExpression(node.initializer))
  )
    return node.parent.parent;
  if (ts.isPropertyDeclaration(node)) return node;
  if (ts.isPropertySignature(node) || ts.isMethodSignature(node)) {
    let owner: ts.Node | undefined = node.parent;
    while (owner && !ts.isSourceFile(owner)) {
      if (ts.isTypeAliasDeclaration(owner)) return node;
      if (ts.isInterfaceDeclaration(owner)) return node;
      owner = owner.parent;
    }
  }
  return null;
}

/** Returns exact undocumented-declaration identities for a repository-relative file path. */
export function analyzeDocumentation(source: ts.SourceFile, file: string): string[] {
  const documentation: string[] = [];
  visit(source, (node) => {
    const documented = documentationTarget(node);
    if (documented && ts.getJSDocCommentsAndTags(documented).length === 0) {
      documentation.push(
        JSON.stringify([
          file,
          "jsdoc",
          ts.SyntaxKind[node.kind],
          (node as ts.NamedDeclaration).name?.getText(source) ?? "constructor",
          findingScope(node, source),
        ]),
      );
    }
  });
  return documentation.sort();
}
