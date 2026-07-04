import type { ReactNode } from 'react'
export interface ControlPanelPage {
  id: string
  title: string
  description: string
}

interface LayoutProps {
  activePageId: string
  children: ReactNode
  onSelectPage: (pageId: string) => void
  pages: ControlPanelPage[]
}

export function Layout({ activePageId, children, onSelectPage, pages }: LayoutProps) {
  return (
    <div className="app-shell">
      <aside className="sidebar" aria-label="Mooncache console">
        <a className="brand" href="#overview" aria-label="Mooncache overview">
          Mooncache
        </a>
        <nav aria-label="Primary">
          <ul>
            {pages.map((page) => (
              <li key={page.id}>
                <a
                  href={`#${page.id}`}
                  aria-current={page.id === activePageId ? 'page' : undefined}
                  onClick={(event) => {
                    event.preventDefault()
                    onSelectPage(page.id)
                  }}
                >
                  {page.title}
                </a>
              </li>
            ))}
          </ul>
        </nav>
      </aside>
      <main className="content" id="main-content">
        {children}
      </main>
    </div>
  )
}
